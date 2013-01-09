package uk.bl.monitrix.model;

import java.util.Iterator;


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
	 * Returns a single base-resolution unit of the aggregated crawl stats.
	 * @param timestamp the timestamp of the unit
	 * @return the crawl stats unit
	 */
	public CrawlStatsUnit getStatsForTimestamp(long timestamp);

}
