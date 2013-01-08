package uk.bl.monitrix.db;

import java.util.Iterator;

import uk.bl.monitrix.CrawlLogEntry;

/**
 * A minimal connection interface for write access to the Monitrix DB.
 * 
 * Note: I'm separating read and write primarily to keep the line-count
 * lower on the implementation classes (i.e. for better readability).
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface IngestConnector {
	
	/**
	 * TODO need to change this, since we want to work in 'tail -f'-like mode
	 * @param iterator
	 */
	public void insert(Iterator<CrawlLogEntry> iterator);

}
