package uk.bl.monitrix.database;

import java.util.List;

import uk.bl.monitrix.model.AlertLog;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.CrawlStats;
import uk.bl.monitrix.model.KnownHost;
import uk.bl.monitrix.model.VirusLog;

/**
 * A connection interface for read access to the Monitrix DB.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface DBConnector {
	
	/**
	 * Returns DB-backed crawl log.
	 * @return the crawl log
	 */
	public CrawlLog getCrawlLog();

	/**
	 * Returns DB-backed crawl stats.
	 * @return the crawl stats
	 */
	public CrawlStats getCrawlStats();

	/**
	 * Returns the DB-backed alert log.
	 * @return the alert log
	 */
	public AlertLog getAlertLog();

	/**
	 * Returns the DB-backed known host record.
	 * @param hostname the hostname
	 * @return the host record
	 */
	public KnownHost getKnownHost(String hostname);
	
	/**
	 * Searches the known hosts list. Supported types of queries depend
	 * on the type of backend!
	 * @param query the query
	 * @return list of host names
	 */
	public List<String> searchHosts(String query);
	
	/**
	 * Returns the DB-backed virus log.
	 * @return the virus log
	 */
	public VirusLog getVirusLog();

	/**
	 * Closes the connection
	 */
	public void close();

}
