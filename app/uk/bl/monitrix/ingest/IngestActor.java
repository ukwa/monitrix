package uk.bl.monitrix.ingest;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;

import uk.bl.monitrix.database.DBBatchImporter;
import uk.bl.monitrix.heritrix.IncrementalLogfileReader;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;

import static akka.dispatch.Futures.future;

/**
 * An actor that handles ingest for a single Heritrix log file.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class IngestActor extends UntypedActor {	
	
	public enum Messages { START, GET_STATUS, STOP }
	
	private File heritrixLog;
	
	private DBBatchImporter importer;
	
	private ActorSystem system;
	
	private IncrementalLogfileReader logReader;
	
	private long linesInLogfile;
	
	private IngestorStatus status = new IngestorStatus(IngestorStatus.Phase.CATCHING_UP);
	
	private boolean keepRunning = true;
	
	public IngestActor(File heritrixLog, DBBatchImporter importer, ActorSystem system) throws FileNotFoundException {
		this.heritrixLog = heritrixLog;
		this.importer = importer;
		this.system = system;
	}
	
	@Override
	public void preStart() {
		try {
			this.logReader = new IncrementalLogfileReader(heritrixLog.getAbsolutePath());
			this.linesInLogfile = countLines(heritrixLog);
		} catch (IOException e) {
			// Should never happen
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void onReceive(Object message) throws Exception {
		if (message.equals(Messages.START)) {
			// Start the async ingest
			future(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					while (keepRunning) {
						status.phase = IngestorStatus.Phase.CATCHING_UP;
						importer.insert(logReader.newIterator());						
						status.phase = IngestorStatus.Phase.TRACKING;
						status.progress = 0;
						Thread.sleep(15000);
					}
					return null;
				}
			}, system.dispatcher());
		} else if (message.equals(Messages.GET_STATUS)) {
			// While in CATCHING_UP phase, update progress
			if (status.phase.equals(IngestorStatus.Phase.CATCHING_UP))
				status.progress = (int) ((100 * logReader.getNumberOfLinesRead()) / linesInLogfile);
				
			getSender().tell(status);
		} else if (message.equals(Messages.STOP)) {
			this.keepRunning = false;
		}
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
	private static int countLines(File file) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(file));
		
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
