package uk.bl.monitrix.model;

import java.util.List;

/**
 * The ingest schedule holds the list of currently ingested logs.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public abstract class IngestSchedule {
	
	/**
	 * Adds a log to the ingest schedule.
	 * @param path the path of the log file
	 * @param crawlerId the Crawler ID connected to that log
	 * @param monitor flag indicating whether this log should be monitored continuously after initial ingest
	 * @return the {@link IngestedLog}
	 */
	public abstract IngestedLog addLog(String path, String crawlerId, boolean monitor);
	
	/**
	 * Returns the list of ingested logs.
	 * @return the logs
	 */
	public abstract List<IngestedLog> getLogs();
	
	/**
	 * Gets the log with the specified ID
	 * @param id the ID
	 * @return the log
	 */
	public abstract IngestedLog getLog(String id);
	
	/**
	 * Returns the log for a specified file path
	 * @param path the file path
	 * @return the {@link IngestedLog}
	 */
	public abstract IngestedLog getLogForPath(String path);
	
	/**
	 * Returns the log for a specified crawler ID
	 * @param crawlerId the crawlerId
	 * @return the {@link IngestedLog}
	 */
	public abstract IngestedLog getLogForCrawlerId(String crawlerId);
	/**
	 * Tests if monitoring is enabled for a specified log ID.
	 * @param id the log ID
	 * @return <code>true</code> if continuous monitoring is enabled for the log
	 */
	public abstract boolean isMonitoringEnabled(String id);
	
	/**
	 * Sets continuous monitoring for a specific log.
	 * @param id the log ID
	 * @param monitoringEnabled flag to toggle continuous monitoring for this log
	 */
	public abstract void setMonitoringEnabled(String id, boolean monitoringEnabled);
	
	/**
	 * Returns the total number of ingested lines successfully recorded to the ingest
	 * schedule. Compare this value against the actual number of lines in the {@link CrawlLog}
	 * for a quick integrity/plausibility check. 
	 * @return the total number of ingested lines recorded in the {@link IngestSchedule}
	 */
	public long getLinesIngested() {
		long total = 0;
		for (IngestedLog log : getLogs())
			total += log.getIngestedLines();
		return total;
	}

}
