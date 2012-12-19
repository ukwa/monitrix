package uk.bl.monitrix;

import java.util.List;

/**
 * The Crawl Statistics interface. Provides access to all global and aggregate
 * crawl analytics data extracted from the Heritrix logs.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public abstract class CrawlStatistics {
	
	/**
	 * Returns the UNIX timestamp of the crawl start time.
	 * @return crawl start time
	 */
	public abstract long getCrawlStartTime();
	
	/**
	 * Returns the UNIX timestamp of the last crawl activity, i.e.
	 * the timestamp of the last log entry written to the DB.
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
	 * Returns the datavolume timeseries, i.e. the amount of data that has been downloaded
	 * over the duration of the crawl. The timeseries will be resampled from its internal
	 * base resolution, so that the result has at most <code>maxDatapoints</code> data
	 * points.  
	 * 
	 * @param maxDatapoints the maximum number of datapoints the timeseries should have
	 * @return the data volume timeseries
	 */
	public abstract List<TimeseriesValue> getDatavolumeHistory(int maxDatapoints);
	
	/**
	 * Returns the crawled URLs timeseries, i.e. the number of URLs visited over the duration
	 * of the crawl. The timeseries will be resampled from its internal base resolution, so
	 * that the result has at most <code>maxDatapoints</code> data points.  
	 * 
	 * @param maxDatapoints the maximum number of datapoints the timeseries should have
	 * @return the crawled URLs timeseries
	 */
	public abstract List<TimeseriesValue> getCrawledURLsHistory(int maxDatapoints);
	
	/**
	 * Returns the new hosts timeseries, i.e. the number of hosts that were visited for the 
	 * first time over the duration of the crawl. The timeseries will be resampled from its
	 * internal base resolution, so that the result has at most <code>maxDatapoints</code> 
	 * data points.  
	 * 
	 * @param maxDatapoints the maximum number of datapoints the timeseries should have
	 * @return the new hosts timeseries
	 */
	public abstract List<TimeseriesValue> getNewHostsCrawledHistory(int maxDatapoints);

}
