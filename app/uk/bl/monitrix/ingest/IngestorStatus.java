package uk.bl.monitrix.ingest;

/**
 * Encapsulates the current status of an {@link IngestActor}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class IngestorStatus {
		
	/**
	 * The phase of operation the {@link IngestActor} is currently in
	 */
	public Phase phase;
	
	/**
	 * The current progress (if applicable in the current phase)
	 */
	public int progress;
	
	public IngestorStatus(Phase phase) {
		this.phase = phase;
		this.progress = 0;
	}
	
	public enum Phase {
	
		/** The ingestor is currently ingesting the next batch of data into the DB **/
		CATCHING_UP,
		
		/** The ingestor is tracking the log, i.e. idle & waiting for the next batch, according to plan **/ 
		TRACKING,
		
		/** The ingestor has terminated **/
		TERMINATED,
		
		/** Unknown status (may be caused by different error situations - consult application logs! **/
		UNKNOWN
	
	}

}
