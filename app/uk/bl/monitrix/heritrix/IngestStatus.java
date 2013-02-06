package uk.bl.monitrix.heritrix;

/**
 * Encapsulates the current status of an {@link OldIngestActor}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class IngestStatus {
		
	/**
	 * The phase of operation the {@link OldIngestActor} is currently in
	 */
	public Phase phase;
	
	/**
	 * The current progress (if applicable in the current phase)
	 */
	public int progress;
	
	public IngestStatus(Phase phase) {
		this.phase = phase;
		this.progress = 0;
	}
	
	public enum Phase {
		
		PENDING,
	
		/** The ingestor is currently ingesting the next batch of data into the DB **/
		CATCHING_UP,
		
		/** The ingestor has caught up and is idle, waiting for the next synchronization ingest **/ 
		IDLE,
		
		/** The ingestor is ingesting the next batch to synchronize the DB with the log **/
		SYNCHRONIZING,
		
		/** The ingestor has terminated **/
		TERMINATED,
		
		/** Unknown status (may be caused by different error situations - consult application logs! **/
		UNKNOWN
	
	}

}
