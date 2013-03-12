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
	 * Returns the log file IDs that occur in the DB.
	 * @return the list of log IDs
	 */
	public abstract List<String> listLogIds();
	
	/**
	 * Returns the total number of log entries received from a specific crawler log file.
	 * @param logPath the path to the source log file
	 * @return the number of entries in the DB originating from that file
	 */
	public abstract long countEntriesForLog(String logId);
	
	/**
	 * Returns all log entries that exist for the specified URL.
	 * @param url the url
	 * @return the log entries
	 */
	public abstract List<CrawlLogEntry> getEntriesForURL(String url);
	
	/**
	 * Searches the crawl log with the specified (e.g. keyword) query.
	 * Refer to documentation of specific implementations for the types of 
	 * queries supported! (Note: on MongoDB only *exacty matches* are supported! This
	 * means that - effectively - the results for .searchURLs and .getEntriesForURL
	 * return identical data!)
	 * @param query the search query
	 * @param limit the max number of results to return
	 * @param offset the result page offset
	 * @return the search result
	 */
	public abstract SearchResult searchURLs(String query, int limit, int offset);
	
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
	
	/**
	 * Counts the log entries that carry the specified annotation.
	 * @param annotation the annotation
	 * @return the number of entries with that annotation
	 */
	public abstract long countEntriesWithAnnotation(String annotation);
	
	/**
	 * Returns the log entries that carry the specified annotation
	 * @param annotation the annotation
	 * @return the log entries with that annotation
	 */
	public abstract Iterator<CrawlLogEntry> getEntriesWithAnnotation(String annotation);
	
}
