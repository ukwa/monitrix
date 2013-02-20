package uk.bl.monitrix.database;

import uk.bl.monitrix.model.AlertLog;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.CrawlStats;
import uk.bl.monitrix.model.IngestSchedule;
import uk.bl.monitrix.model.KnownHostList;
import uk.bl.monitrix.model.VirusLog;

/**
 * A connection interface for read access to the Monitrix DB.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface DBConnector {
	
	/**
	 * Returns the DB-backed ingest schedule.
	 * @return the ingest schedule
	 */
	public IngestSchedule getIngestSchedule();
	
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
	 * Returns the DB-backed known host list.
	 * @return the known host list
	 */
	public KnownHostList getKnownHostList();
	
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
