package uk.bl.monitrix.db;

import java.util.List;

import uk.bl.monitrix.api.AlertLog;
import uk.bl.monitrix.api.CrawlLog;
import uk.bl.monitrix.api.CrawlStatistics;
import uk.bl.monitrix.api.HostInformation;

/**
 * A minimal connection interface for read access to the Monitrix DB.
 * 
 * Note: I'm separating read and write primarily to keep the line-count
 * lower on the implementation classes (i.e. for better readability).
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface ReadConnector {
	
	/**
	 * Returns DB-backed crawl log.
	 * @return the crawl log
	 */
	public CrawlLog getCrawlLog();
	
	/**
	 * Returns DB-backed crawl statistics.
	 * @return the crawl statistics
	 */
	public CrawlStatistics getCrawlStatistics();
	
	/**
	 * Searches the known hosts list. Supported types of queries depend
	 * on the type of backend!
	 * @param query the query
	 * @return list of host names
	 */
	public List<String> searchHosts(String query);
	
	/**
	 * Returns the DB-backed alert log.
	 * @return the alert log
	 */
	public AlertLog getAlertLog();
	
	/**
	 * Returns DB-backed host information
	 * @param hostname the host name
	 * @return the host information
	 */
	public HostInformation getHostInfo(String hostname);
	
	/**
	 * Closes the connection
	 */
	public void close();

}
