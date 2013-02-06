package uk.bl.monitrix.heritrix;

import java.io.BufferedInputStream;
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
import uk.bl.monitrix.heritrix.IngestStatus.Phase;

import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import akka.dispatch.Futures;

public class IngestActor extends UntypedActor {
	
	private DBIngestConnector db;
	
	private ActorSystem system;
	
	private static long sleepInterval = 15000;
	
	private Map<String, IncrementalLogfileReader> newLogs = new HashMap<String, IncrementalLogfileReader>(); 
	
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
					status.progress = (int) ((100 * reader.getNumberOfLinesRead()) / countLines(reader.getPath()));
				}
			}
			getSender().tell(statusList);
		} else if (msg.getCommand().equals(IngestControlMessage.Command.ADD_WATCHED_LOG)) {
			String path = (String) msg.getPayload();
			statusList.put(path, new IngestStatus(Phase.PENDING));
			IncrementalLogfileReader reader = new IncrementalLogfileReader(path);
			newLogs.put(path, reader);
		} else if (msg.getCommand().equals(IngestControlMessage.Command.CHANGE_SLEEP_INTERVAL)) {
			Long newInterval = (Long) msg.getPayload();
			sleepInterval = newInterval.longValue();
		}
	}
	
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
		long linesTotal = countLines(reader.getPath());
		long linesToSkip = db.countEntriesForLog(reader.getPath());
		
		IngestStatus status = statusList.get(reader.getPath());
		if (linesToSkip < linesTotal) {
			Logger.info("Skipping " + linesToSkip + " of " + linesTotal + " log lines");
			
			status.phase = IngestStatus.Phase.CATCHING_UP;
			
			Iterator<LogFileEntry> iterator = reader.newIterator();
			for (long i=0; i<linesToSkip; i++) {
				if (iterator.hasNext())
					iterator.next();
			}
		
			Logger.info("Catching up with log file contents");
			db.insert(reader.getPath(), reader.newIterator());
		} else {
			Logger.info("Log is fully ingested - no need to catch up.");
		}
		
		status.phase = IngestStatus.Phase.IDLE;
		status.progress = 0;
	}
	
	private void synchronizeWithLog(IncrementalLogfileReader reader) {
		IngestStatus status = statusList.get(reader.getPath());
		status.phase = IngestStatus.Phase.SYNCHRONIZING;		
		db.insert(reader.getPath(), reader.newIterator());		
		status.phase = IngestStatus.Phase.IDLE;
	}
	
	/**
	 * Counts the lines of a log file. Seems to be the fastest way to implement this.
	 * Taken from:
	 * 
	 * http://stackoverflow.com/questions/453018/number-of-lines-in-a-file-in-java
	 * 
	 * @param file the file
	 * @return the number of lines in the file
	 * @throws IOException if anything goes wrong
	 */
	private static int countLines(String path) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(path));
		
	    try {
	        byte[] c = new byte[1024];
	        int count = 0;
	        int readChars = 0;
	        boolean empty = true;
	        while ((readChars=is.read(c)) != -1) {
	            empty = false;
	            for (int i=0; i<readChars; ++i) {
	                if (c[i] == '\n')
	                    ++count;
	            }
	        }
	        return (count == 0 && !empty) ? 1 : count;
	    } finally {
	        is.close();
	    }
	}

}
