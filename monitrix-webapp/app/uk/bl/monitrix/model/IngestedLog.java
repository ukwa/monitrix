package uk.bl.monitrix.model;

/**
 * The IngestedLog domain object interface. Encapsulates information about a log that is ingested into
 * monitrix.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface IngestedLog {
	
	/**
	 * An internal ID for the log.
	 * @return the log ID
	 */
	public String getId();
	
	/**
	 * The path of the log file.
	 * @return the file path
	 */
	public String getPath();
	
	/**
	 * The crawler ID assigned to this log file.
	 * @return the crawler ID
	 */
	public String getCrawlerId();
	
	/**
	 * Returns the number of log lines ingested from this log
	 * @return the number of ingested lines
	 */
	public long getIngestedLines();
	
	/**
	 * Returns <code>true</code> if the log is being monitored for changes.
	 * @return <code>true</code> if the log is being monitored
	 */
	public boolean isMonitored();
	
}
