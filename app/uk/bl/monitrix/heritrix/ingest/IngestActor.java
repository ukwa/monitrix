package uk.bl.monitrix.heritrix.ingest;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
	private static final int LINE_NUMBER_ESTIMATION_SAMPLE_SIZE = 100000;
	
	private DBIngestConnector db;
	
	private ActorSystem system;
	
	private static long sleepInterval = 15000;
	
	private Map<String, IncrementalLogfileReader> newLogs = new HashMap<String, IncrementalLogfileReader>();
	
	// private Map<String, Long> bufferedLineCounts = new HashMap<String, Long>();
	private Map<String, Long> estimatedLineCounts = new HashMap<String, Long>();
	
	private Map<String, IncrementalLogfileReader> watchedLogs = new HashMap<String, IncrementalLogfileReader>();
	
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
					IncrementalLogfileReader reader = newLogs.get(entry.getKey());
					status.progress = (int) ((100 * reader.getNumberOfLinesRead()) / estimatedLineCounts.get(entry.getKey()));
				}
			}

			getSender().tell(statusList);
		} else if (msg.getCommand().equals(IngestControlMessage.Command.ADD_WATCHED_LOG)) {
			String path = (String) msg.getPayload();
			statusList.put(path, new IngestStatus(Phase.PENDING));
			estimateLinesAsync(path, LINE_NUMBER_ESTIMATION_SAMPLE_SIZE);
		} else if (msg.getCommand().equals(IngestControlMessage.Command.CHANGE_SLEEP_INTERVAL)) {
			Long newInterval = (Long) msg.getPayload();
			sleepInterval = newInterval.longValue();
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
	private void estimateLinesAsync(final String path, final int maxLines) {
		Future<Long> f = Futures.future(new Callable<Long>() {
			@Override
			public Long call() throws Exception {
				Logger.info("Estimating number of lines for " + path);
				
				// Count number of characters in the first N lines
				// long characters = countCharacters(path, maxLines);
				
				InputStream is = new BufferedInputStream(new FileInputStream(path));
				
				long characters = 0;
			    try {
			        byte[] buffer = new byte[4096];
			        int lineCount = 0;
			        
			        int readChars = 0;
			        while ((readChars = is.read(buffer)) != -1 && lineCount < maxLines) {
			            characters += readChars;
			            for (int i=0; i<readChars; ++i) {
			                if (buffer[i] == '\n')
			                    ++lineCount;
			            }
			        }
			    } finally {
			        is.close();
			    }
				
				Logger.info("Sample lines take up " + characters / (1024 * 1024) + " MB (assuming 1 byte per character)");
				
				File f = new File(path);
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
				estimatedLineCounts.put(path, estimatedLineCount);
				IncrementalLogfileReader reader = new IncrementalLogfileReader(path);
				newLogs.put(path, reader);
			}		
		});
	}
	
	/**
	 * Detects a text file's charset (so that we know how many bytes are used per character
	 * for line number estimation).
	 * @param path the file path
	 * @param maxChars the maximum number of characters to sample from the file
	 * @return the charset name or <code>null</code>
	 * @throws IOException if anything goes wrong reading the file
	 *
	private String detectCharset(String path, int maxChars) throws IOException  {
		byte[] buffer = new byte[4096];
		System.out.println("Starting charset detection");
		
		FileInputStream fis = new FileInputStream(path);
		System.out.println("Got the input stream");
		
		UniversalDetector detector = new UniversalDetector(null);
		
		int charCount = 0;
		int nread;
		while ((nread = fis.read(buffer)) > 0 && !detector.isDone() && charCount < maxChars) {
			charCount += nread;
			System.out.println("charcount = " + charCount);
			detector.handleData(buffer, 0, nread);
		}
		detector.dataEnd();
		
		System.out.println("done.");
		
		String charset = detector.getDetectedCharset();
		detector.reset();
		fis.close();
		
		return charset;
	}
	*/
	
	/**
	 * Counts the number of characters contained in the first N lines of a file (or less, if the file
	 * has less lines). 
	 * @param path the file path
	 * @param maxLines the number of lines for which to count characters
	 * @return the number of characters in (up to) the first <code>maxLines</code> lines
	 * @throws IOException if anything goes wrong reading the file
	 *
	private long countCharacters(String path, int maxLines) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(path));
		
	    try {
	        byte[] buffer = new byte[4096];
	        
	        long characterCount = 0;
	        int lineCount = 0;
	        
	        int readChars = 0;
	        while ((readChars = is.read(buffer)) != -1 && lineCount < maxLines) {
	            characterCount += readChars;
	            for (int i=0; i<readChars; ++i) {
	                if (buffer[i] == '\n')
	                    ++lineCount;
	            }
	        }
	        
	        return characterCount;
	    } finally {
	        is.close();
	    }		
	}
	*/
	
	private void startSynchronizationLoop() throws InterruptedException, IOException {
		Futures.future(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				Logger.info("Starting log synchronization loop");
				while (keepRunning) {
					// Check if there are new logs to watch - and ingest if any
					List<IncrementalLogfileReader> toAdd = new ArrayList<IncrementalLogfileReader>();
					for (IncrementalLogfileReader reader : newLogs.values())
						toAdd.add(reader);
					
					for (IncrementalLogfileReader reader : toAdd) {
						Logger.info("Catching up with log file: " + reader.getPath());
						catchUpWithLog(reader);
						newLogs.remove(reader.getPath());
					}
					
					// Sync all other logs
					for (IncrementalLogfileReader reader : watchedLogs.values()) {
						Logger.info("Synchronizing log: " + reader.getPath());
						synchronizeWithLog(reader);
					}
					
					// Add new Logs to synchroniziation loop
					for (IncrementalLogfileReader reader : toAdd)
						watchedLogs.put(reader.getPath(), reader);
					
					// Go to sleep
					Thread.sleep(sleepInterval);
				}
				
				return null;
			}
		}, system.dispatcher());
	}
	
	private void catchUpWithLog(IncrementalLogfileReader reader) throws IOException {
		long estimatedLinesTotal = estimatedLineCounts.get(reader.getPath());
		long linesToSkip = db.countEntriesForLog(reader.getPath());
		
		IngestStatus status = statusList.get(reader.getPath());
		Logger.info("Skipping " + linesToSkip + " (of estimated " + estimatedLinesTotal + " log lines)");
					
		status.phase = IngestStatus.Phase.CATCHING_UP;
			
		Iterator<LogFileEntry> iterator = reader.newIterator();
		for (long i=0; i<linesToSkip; i++) {
			if (iterator.hasNext())
				iterator.next();
		}
	
		Logger.info("Catching up with log file contents");
		db.insert(reader.getPath(), iterator);
		
		status.phase = IngestStatus.Phase.IDLE;
		status.progress = 0;
	}
	
	private void synchronizeWithLog(IncrementalLogfileReader reader) {
		IngestStatus status = statusList.get(reader.getPath());
		status.phase = IngestStatus.Phase.SYNCHRONIZING;		
		db.insert(reader.getPath(), reader.newIterator());		
		status.phase = IngestStatus.Phase.IDLE;
	}

}
