package controllers.mapping;

import uk.bl.monitrix.model.CrawlStatsUnit;

/**
 * A simple class that wraps a {@link CrawlStatsUnit} so that it can be directly 
 * serialized to JSON by Play. 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CrawlStatsUnitMapper {
	
	public long timestamp;
	
	public long crawl_rate;
	
	public long download_rate;
	
	public long new_hosts_crawled;
	
	public CrawlStatsUnitMapper(CrawlStatsUnit unit) {
		this.timestamp = unit.getTimestamp();
		this.crawl_rate = unit.getNumberOfURLsCrawled();
		this.download_rate = unit.getDownloadVolume();
		this.new_hosts_crawled = unit.getNumberOfNewHostsCrawled();
	}

}
