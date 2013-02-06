package uk.bl.monitrix.model;

import java.util.Iterator;
import java.util.List;

/**
 * The crawl log interface.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public abstract class CrawlLog {
	
	/**
	 * Returns the UNIX timestamp of the crawl start time, i.e.
	 * the timestamp of the first entry written to the log.
	 * @return crawl start time
	 */
	public abstract long getCrawlStartTime();
	
	/**
	 * Returns the UNIX timestamp of the last crawl activity, i.e.
	 * the timestamp of the last entry in written to the log.
	 * @return last crawl activity timestamp
	 */
	public abstract long getTimeOfLastCrawlActivity();
	
	/**
	 * Utility method: returns the duration of the crawl so far (in
	 * milliseconds).
	 * @return the duration of the crawl
	 */
	public long getCrawlDuration() {
		return getTimeOfLastCrawlActivity() - getCrawlStartTime();
	}
	
	/**
	 * Utility method: returns true if the last crawl activity was
	 * more than 2 minutes ago (in which case we consider the crawl idle)
	 * @return <code>true</code> if the crawl is idle 
	 */
	public boolean isIdle() {
		return (System.currentTimeMillis() - getTimeOfLastCrawlActivity()) > 120000; 
	}

	/**
	 * Returns the N most recent entries in the log.
	 * @param n the number of entries to return
	 * @return the log entries
	 */
	public abstract List<CrawlLogEntry> getMostRecentEntries(int n);
	
	/**
	 * Returns the total number of log entries.
	 * @return the total number of log entries
	 */
	public abstract long countEntries();
	
	/**
	 * Returns the list of log files (absolute path names) that are ingested in the DB.
	 * @return the list of log file paths
	 */
	public abstract List<String> getIngestedLogs();
	
	/**
	 * Returns the total number of log entries received from a specific crawler log file.
	 * @param logPath the path to the source log file
	 * @return the number of entries in the DB originating from that file
	 */
	public abstract long countEntriesForLog(String logPath);
	
	/**
	 * Counts the log entries for a specific host.
	 * @param hostname the host name
	 * @return the number of log entries for the host
	 */
	public abstract long countEntriesForHost(String hostname);
	
	/**
	 * Returns the log entries for a specific host. 
	 * @param hostname the host name
	 * @return the log entries for the host
	 */
	public abstract Iterator<CrawlLogEntry> getEntriesForHost(String hostname); 
	
}
