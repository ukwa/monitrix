package uk.bl.monitrix.model;

import java.util.List;

/**
 * The ingest schedule holds the list of currently ingested logs.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface IngestSchedule {
	
	/**
	 * Adds a log to the ingest schedule.
	 * @param path the path of the log file
	 * @param crawlerId the Crawler ID connected to that log
	 * @param monitor flag indicating whether this log should be monitored continuously after initial ingest
	 * @return the {@link IngestedLog}
	 */
	public IngestedLog addLog(String path, String crawlerId, boolean monitor);
	
	/**
	 * Returns the list of ingested logs.
	 * @return the logs
	 */
	public List<IngestedLog> getLogs();
	
	/**
	 * Gets the log with the specified ID
	 * @param id the ID
	 * @return the log
	 */
	public IngestedLog getLog(String id);
	
	/**
	 * Returns the log for a specified file path
	 * @param path the file path
	 * @return the {@link IngestedLog}
	 */
	public IngestedLog getLogForPath(String path);
	
	/**
	 * Sets continuous monitoring for a specific log.
	 * @param id the log ID
	 * @param monitoringEnabled flag to toggle continuous monitoring for this log
	 */
	public void setMonitoringEnabled(String id, boolean monitoringEnabled);

}
