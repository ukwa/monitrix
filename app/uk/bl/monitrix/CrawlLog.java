package uk.bl.monitrix;

import java.util.Iterator;
import java.util.List;

import uk.bl.monitrix.heritrix.LogEntry;

/**
 * The Crawl Log interface. Provides access to the raw log
 * data stored in the database.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 *
 */
public interface CrawlLog {

	/**
	 * Returns the N most recent entries in the log stored 
	 * in the database.
	 * @param n the number of entries to return
	 * @return the log entries
	 */
	public List<LogEntry> getMostRecentEntries(int n);
	
	/**
	 * Counts the log entries for a specific host.
	 * @param hostname the host name
	 * @return the number of log entries for the host
	 */
	public long countEntriesForHost(String hostname);
	
	/**
	 * Returns the log entries for a specific host. 
	 * @param hostname the host name
	 * @return the log entries for the host
	 */
	public Iterator<LogEntry> getEntriesForHost(String hostname); 
	
}
