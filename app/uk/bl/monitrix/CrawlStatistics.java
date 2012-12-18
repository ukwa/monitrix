package uk.bl.monitrix;

import java.util.List;

/**
 * Crawl statistics interface.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public abstract class CrawlStatistics {
	
	public abstract long getCrawlStartTime();
	
	public abstract long getTimeOfLastCrawlActivity();
	
	public long getCrawlDuration() {
		return getTimeOfLastCrawlActivity() - getCrawlStartTime();
	}
	
	public abstract List<TimeseriesValue> getDatavolumeHistory(int maxDatapoints);
	
	public abstract List<TimeseriesValue> getCrawledURLsHistory(int maxDatapoints);

}
