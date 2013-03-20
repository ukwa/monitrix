package uk.bl.monitrix.heritrix.ingest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import play.Logger;
import uk.bl.monitrix.database.DBIngestConnector;
import uk.bl.monitrix.heritrix.IncrementalLogfileReader;
import uk.bl.monitrix.heritrix.LogFileEntry;
import uk.bl.monitrix.heritrix.ingest.IngestStatus.Phase;
import uk.bl.monitrix.model.IngestSchedule;
import uk.bl.monitrix.model.IngestedLog;

import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import akka.dispatch.Future;
import akka.dispatch.Futures;
import akka.dispatch.OnSuccess;

/**
 * This class coordinates the actual work of monintoring log files and
 * running incremental ingest.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class IngestActor extends UntypedActor {
	
	// The number of lines to average when estimating total no. of lines per log file
	private static final int LINE_NUMBER_ESTIMATION_SAMPLE_SIZE = 50000;
	
	private DBIngestConnector db;
	
	private ActorSystem system;
	
	private static long sleepInterval = 15000;
	
	private Map<String, WatchedLog> newLogs = new HashMap<String, WatchedLog>();
	
	private Map<String, WatchedLog> watchedLogs = new HashMap<String, WatchedLog>();
	
	private Map<String, IngestStatus> statusList = new HashMap<String, IngestStatus>();

	private boolean isRunning = false;
	
	private boolean keepRunning = true;
	
	public IngestActor(DBIngestConnector db, ActorSystem system) {
		this.db = db;
		this.system = system;
	}
	
	@Override
	public void onReceive(Object arg) throws Exception {
		IngestControlMessage msg = (IngestControlMessage) arg;
		if (msg.getCommand().equals(IngestControlMessage.Command.START)) {
			if (!isRunning) {
				isRunning = true;
				startSynchronizationLoop();
			}
		} else if (msg.getCommand().equals(IngestControlMessage.Command.STOP)) {
			keepRunning = false;
		} else if (msg.getCommand().equals(IngestControlMessage.Command.GET_STATUS)) {
			// Compute progress for logs in CATCHING_UP phase
			for (Entry<String, IngestStatus> entry : statusList.entrySet()) {
				IngestStatus status = entry.getValue();
				if (status.phase.equals(IngestStatus.Phase.CATCHING_UP)) {
					WatchedLog log = newLogs.get(entry.getKey());
					status.progress = (int) ((100 * log.getReader().getNumberOfLinesRead()) / log.getEstimatedLineCount());
				}
			}

			getSender().tell(statusList);
		} else if (msg.getCommand().equals(IngestControlMessage.Command.SYNC_WITH_SCHEDULE)) {
			for (IngestedLog log : db.getIngestSchedule().getLogs()) {
				String id = log.getId();
				if (!watchedLogs.containsKey(id) && !newLogs.containsKey(id)) {
					statusList.put(id, new IngestStatus(Phase.PENDING));
					estimateLinesAsync(log, LINE_NUMBER_ESTIMATION_SAMPLE_SIZE);
				}
			}
			
			// TODO update monitored/not-monitored info
		} else if (msg.getCommand().equals(IngestControlMessage.Command.CHANGE_SLEEP_INTERVAL)) {
			Long newInterval = (Long) msg.getPayload();
			sleepInterval = newInterval.longValue();
		} else if (msg.getCommand().equals(IngestControlMessage.Command.CHECK_RUNNING)) {
			getSender().tell(isRunning);
		}
	}
	
	/**
	 * Estimates the total number of lines in a text file based on computing the byte-size of the first
	 * <code>maxLines</code> lines, and assuming that all other lines will have the same average byte-size.
	 * 
	 * <strong>Warning #1:</strong> This method assumes 8-bit-per-character encoding (e.g. UTF-8)! Detecting 
	 * encoding would be computationally to intensive. If the log file were encoded with a N bytes-per-character,
	 * the result of this method will be too high by the factor N. 
	 * 
	 * <strong>Warning #2:</strong> the result of this method will be too high if the file
	 * is method will yield a result which is too high, if the file has less than <code>maxLines</code> lines.
	 * (This limitation should be irrelevant for the monitrix use case, however.)
	 * 
	 * @param path the file path
	 * @param maxLines the maximum number of lines to sample from the file
	 */
	private void estimateLinesAsync(final IngestedLog log, final int maxLines) {
		Future<Long> f = Futures.future(new Callable<Long>() {
			@Override
			public Long call() throws Exception {
				Logger.info("Estimating number of lines for " + log.getPath());
				
				// Count number of characters in the first N lines
				BufferedReader reader = new BufferedReader(new FileReader(log.getPath()));
				long characters = 0;
			    try {
			    	int lineCount = 0;
			    	String line;
			        while ((line = reader.readLine()) != null && lineCount < maxLines) {
			            characters += line.length();
	                    lineCount++;
			        }
			    } finally {
			        reader.close();
			    }
				
				Logger.info("Sample lines take up " + characters / (1024 * 1024) + " MB (assuming 1 byte per character)");
				
				File f = new File(log.getPath());
				long bytesTotal = f.length();
				Logger.info("File has " + bytesTotal / (1024 * 1024) + " MB total");
				
				double ratio = (double) bytesTotal / (double) characters;
				
				long estimatedLines = (long) (maxLines * ratio);
				Logger.info("Estimated a total number of " + estimatedLines + " lines");
				
				return estimatedLines;
			}
		}, system.dispatcher());
		
		f.onSuccess(new OnSuccess<Long>() {
			@Override
			public void onSuccess(Long estimatedLineCount) throws Throwable {
				WatchedLog watchedLog = new WatchedLog(log, new IncrementalLogfileReader(log.getPath()));
				watchedLog.setEstimatedLineCount(estimatedLineCount);
				newLogs.put(log.getId(), watchedLog);
			}		
		});
	}
	
	private void startSynchronizationLoop() throws InterruptedException, IOException {
		Futures.future(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				Logger.info("Starting log synchronization loop");
				
				IngestSchedule schedule = db.getIngestSchedule();
				
				while (keepRunning) {
					// Check if there are new logs to watch - and ingest if any
					List<WatchedLog> toAdd = new ArrayList<WatchedLog>();
					for (WatchedLog watchedLog : newLogs.values())
						toAdd.add(watchedLog);
					
					for (WatchedLog log : toAdd) {
						// TODO performance improvement
						if (schedule.isMonitoringEnabled(log.getLogInfo().getId())) {
							Logger.info("Catching up with log file " + log.getReader().getPath());
							catchUpWithLog(log);
							Logger.info("Caught up with log file " + log.getReader().getPath());
							newLogs.remove(log.getLogInfo().getId());
						}
					}
					
					// Sync all other logs
					for (WatchedLog log : watchedLogs.values()) {
						// TODO performance improvement
						if (schedule.isMonitoringEnabled(log.getLogInfo().getId())) {
							Logger.debug("Synchronizing log: " + log.getLogInfo().getPath());
							
							// Check if file was renamed in the mean time
							if(log.getReader().isRenamed()) {
								Logger.info("Detected log rename");
								log.setReader(new IncrementalLogfileReader(log.getLogInfo().getPath()));
							}
							
							synchronizeWithLog(log);
						}
					}
					
					// Add new Logs to synchroniziation loop
					for (WatchedLog log : toAdd)
						watchedLogs.put(log.getLogInfo().getId(), log);
					
					// Go to sleep
					Thread.sleep(sleepInterval);
				}
			
				Logger.info("Stopping synchronization loop");
				return null;
			}
		}, system.dispatcher());
	}
	
	private void catchUpWithLog(WatchedLog log) {
		try {
			IncrementalLogfileReader reader = log.getReader();
			long linesToSkip = db.getIngestSchedule().getLogForPath(reader.getPath()).getIngestedLines();
			
			Logger.info("Skipping " + linesToSkip + " (of estimated " + log.getEstimatedLineCount() + " log lines)");
			
			IngestStatus status = statusList.get(log.getLogInfo().getId());			
			status.phase = IngestStatus.Phase.CATCHING_UP;
				
			Iterator<LogFileEntry> iterator = reader.newIterator();
			for (long i=0; i<linesToSkip; i++) {
				if (iterator.hasNext())
					iterator.next();
			}
		
			Logger.info("Catching up with log file contents");
			db.insert(log.getLogInfo().getId(), iterator);
			
			status.phase = IngestStatus.Phase.IDLE;
			status.progress = 0;
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
	
	private void synchronizeWithLog(WatchedLog log) {
		try {
			IngestStatus status = statusList.get(log.getLogInfo().getId());
			status.phase = IngestStatus.Phase.SYNCHRONIZING;		
			db.insert(log.getLogInfo().getId(), log.getReader().newIterator());		
			status.phase = IngestStatus.Phase.IDLE;
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

}
