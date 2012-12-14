package uk.bl.monitrix.db;

import java.util.Iterator;

import uk.bl.monitrix.heritrix.LogEntry;

/**
 * A minimal DB connection interface, so we're prepared to switch storage backends.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface DBConnector {
	
	// TODO needs to change eventually, since we want to work in 'tail -f'-like mode
	public void insert(Iterator<LogEntry> iterator);
	
	public CrawlStatistics getCrawlStatistics();
	
	public void close();

}
