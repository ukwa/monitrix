package uk.bl.monitrix;

import java.util.List;

/**
 * Crawl statistics interface.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface CrawlStatistics {
	
	public long getCrawlStartTime();
	
	public long getTimeOfLastCrawlActivity();
	
	public List<TimeseriesValue> getDatavolumeHistory(int maxDatapoints);
	
	public List<TimeseriesValue> getCrawledURLsHistory(int maxDatapoints);

}
