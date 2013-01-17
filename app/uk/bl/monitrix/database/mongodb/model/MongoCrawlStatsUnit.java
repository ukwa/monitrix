package uk.bl.monitrix.database.mongodb.model;

import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.CrawlStatsUnit;

/**
 * A MongoDB-backed implementation of {@link CrawlStatsUnit}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoCrawlStatsUnit extends CrawlStatsUnit {
	
	private DBObject dbo;

	public MongoCrawlStatsUnit(DBObject dbo) {
		this.dbo = dbo;
	}
	
	/**
	 * Returns the MongoDB entity that's backing this object.
	 * @return the DBObject
	 */
	public DBObject getBackingDBO() {
		return dbo;
	}

	@Override
	public long getTimestamp() {
		return (Long) dbo.get(MongoProperties.FIELD_CRAWL_STATS_TIMESTAMP);
	}
	
	public void setTimestamp(long timestamp) {
		dbo.put(MongoProperties.FIELD_CRAWL_STATS_TIMESTAMP, timestamp);
	}

	@Override
	public int getDownloadVolume() {
		return (Integer) dbo.get(MongoProperties.FIELD_CRAWL_STATS_DOWNLOAD_VOLUME);
	}
	
	public void setDownloadVolume(int volume) {
		dbo.put(MongoProperties.FIELD_CRAWL_STATS_DOWNLOAD_VOLUME, volume);
	}

	@Override
	public long getNumberOfURLsCrawled() {
		return (Long) dbo.get(MongoProperties.FIELD_CRAWL_STATS_NUMBER_OF_URLS_CRAWLED);
	}
	
	public void setNumberOfURLsCrawled(long numberOfURLs) {
		dbo.put(MongoProperties.FIELD_CRAWL_STATS_NUMBER_OF_URLS_CRAWLED, numberOfURLs);
	}

	@Override
	public long getNumberOfNewHostsCrawled() {
		return (Long) dbo.get(MongoProperties.FIELD_CRAWL_STATS_NEW_HOSTS_CRAWLED);
	}
	
	public void setNumberOfNewHostsCrawled(long newHostsCrawled) {
		dbo.put(MongoProperties.FIELD_CRAWL_STATS_NEW_HOSTS_CRAWLED, newHostsCrawled);
	}

	@Override
	public int countCompletedHosts() {
		Integer count = (Integer) dbo.get(MongoProperties.FIELD_CRAWL_STATS_COMPLETED_HOSTS);
		if (count == null)
			return 0;
		
		return count.intValue();
	}
	
	public void setCompletedHosts(int completedHosts) {
		dbo.put(MongoProperties.FIELD_CRAWL_STATS_COMPLETED_HOSTS, completedHosts);
	}

}
