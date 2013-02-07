package uk.bl.monitrix.heritrix.ingest;

/**
 * Encapsulates the current status of an {@link IngestActor}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class IngestStatus {
		
	/**
	 * The phase of operation the {@link IngestActor} is currently in
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
		
		/** The ingest actor is currently pending, waiting to catch up with the log **/
		PENDING,
	
		/** The ingest actor is currently ingesting the next batch of data into the DB **/
		CATCHING_UP,
		
		/** The ingest actor has caught up and is idle, waiting for the next sync round **/ 
		IDLE,
		
		/** The ingest actor is ingesting the next batch to sync the DB with the log **/
		SYNCHRONIZING,
		
		/** The ingestor has terminated **/
		TERMINATED,
		
		/** Unknown status (may be caused by different error situations - consult application logs! **/
		UNKNOWN
	
	}

}
