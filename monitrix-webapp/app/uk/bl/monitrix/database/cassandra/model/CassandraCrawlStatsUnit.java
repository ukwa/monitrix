package uk.bl.monitrix.database.cassandra.model;

import com.datastax.driver.core.Row;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.CrawlStatsUnit;

/**
 * A CassandraDB-backed implementation of {@link CrawlStatsUnit}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraCrawlStatsUnit extends CrawlStatsUnit {
	
	private Row row;

	public CassandraCrawlStatsUnit(Row row) {
		this.row = row;
	}
	
	@Override
	public long getTimestamp() {
		return row.getDate(CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP).getTime();
	}
	
	@Override
	public long getDownloadVolume() {
		return row.getLong(CassandraProperties.FIELD_CRAWL_STATS_DOWNLOAD_VOLUME);
	}
	
	@Override
	public long getNumberOfURLsCrawled() {
		return row.getLong(CassandraProperties.FIELD_CRAWL_STATS_NUMBER_OF_URLS_CRAWLED);
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
	
}
