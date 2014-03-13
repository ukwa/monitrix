package uk.bl.monitrix.database.cassandra.model;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.CrawlStatsUnit;

/**
 * A CassandraDB-backed implementation of {@link CrawlStatsUnit}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraCrawlStatsUnit extends CrawlStatsUnit {

	private static final String TABLE= CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_CRAWL_STATS;
	
	private Row row;
	
	private String crawlId;
	
	private long timestamp;
	
	private long downloadVolume;
	
	private long numberOfURLsCrawled;

	public CassandraCrawlStatsUnit(Row row) {
		this.row = row;
		this.crawlId = row.getString(CassandraProperties.FIELD_CRAWL_STATS_CRAWL_ID);
		this.timestamp = row.getLong(CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP);
		this.downloadVolume = row.getLong(CassandraProperties.FIELD_CRAWL_STATS_DOWNLOAD_VOLUME);
		this.numberOfURLsCrawled = row.getLong(CassandraProperties.FIELD_CRAWL_STATS_NUMBER_OF_URLS_CRAWLED);
	}
	
	public String getCrawlID() {
		return crawlId;
	}
	
	@Override
	public long getTimestamp() {
		return timestamp;
	}
	
	@Override
	public long getDownloadVolume() {
		return downloadVolume;
	}
	
	public void setDownloadVolume(long volume) {
		this.downloadVolume = volume;
	}
	
	@Override
	public long getNumberOfURLsCrawled() {
		return numberOfURLsCrawled;
	}
	
	public void setNumberOfURLsCrawled(long urls) {
		this.numberOfURLsCrawled = urls;
	}
	
	@Override
	public long getNumberOfNewHostsCrawled() {
		return row.getLong(CassandraProperties.FIELD_CRAWL_STATS_NEW_HOSTS_CRAWLED);
	}
	
	@Override
	public long countCompletedHosts() {
		Long count = row.getLong(CassandraProperties.FIELD_CRAWL_STATS_COMPLETED_HOSTS);
		if (count == null)
			return 0;
		
		return count.intValue();
	}
	
	public void save(Session session) {
		session.execute("UPDATE " + TABLE + " SET " + CassandraProperties.FIELD_CRAWL_STATS_NUMBER_OF_URLS_CRAWLED + "=" + numberOfURLsCrawled +
				", " + CassandraProperties.FIELD_CRAWL_STATS_DOWNLOAD_VOLUME + "=" + downloadVolume +
				" WHERE " + CassandraProperties.FIELD_CRAWL_STATS_CRAWL_ID + "='" + crawlId + "' AND " + 
				CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP + "=" + timestamp + ";");
	}
	
}
