package uk.bl.monitrix.db;

import java.util.Iterator;

import uk.bl.monitrix.CrawlStatistics;
import uk.bl.monitrix.heritrix.LogEntry;

/**
 * A minimal DB connection interface, so we're prepared to switch storage backends.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface DBConnector {
	
	/**
	 * TODO need to change this, since we want to work in 'tail -f'-like mode
	 * @param iterator
	 */
	public void insert(Iterator<LogEntry> iterator);
	
	/**
	 * Returns crawl statistics.
	 * @return the crawl statistics
	 */
	public CrawlStatistics getCrawlStatistics();
	
	/**
	 * Closes the connection
	 */
	public void close();

}
