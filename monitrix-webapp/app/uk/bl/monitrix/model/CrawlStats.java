package uk.bl.monitrix.model;

import java.util.Iterator;
import java.util.List;

/**
 * The crawl stats interface. Provides read/query access to aggregate crawl 
 * analytics data extracted from the logs.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface CrawlStats {
	
	/**
	 * Returns an iterator over the entire aggregated crawl stats.
	 * @return the crawl stats
	 */
	public Iterator<CrawlStatsUnit> getCrawlStats();
	
	/**
	 * Returns an iterator over the stats for a particular crawl.
	 * 
	 * @param crawl_id of the crawl - returns data for all crawls if NULL.
	 * @return the crawl stats
	 */
	public Iterator<CrawlStatsUnit> getCrawlStats( String crawl_id );
	
	/**
	 * Returns a single base-resolution unit of the aggregated crawl stats.
	 * @param timestamp the timestamp of the unit
	 * @param crawl_id of the crawl 
	 * @return the crawl stats unit
	 */
	public CrawlStatsUnit getStatsForTimestamp(long timestamp, String crawl_id);
	
	/**
	 * Returns the N most recent recorded stats. 
	 * @param n the number of timestamp units to return
	 * @return the stats units
	 */
	public List<CrawlStatsUnit> getMostRecentStats(int n);

}
