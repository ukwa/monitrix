package uk.bl.monitrix.database.cassandra.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.CrawlStatsUnit;

/**
 * A CassandraDB-backed implementation of {@link CrawlStatsUnit}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraCrawlStatsUnit extends CrawlStatsUnit {

	private static final String TABLE = CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_CRAWL_STATS;

	private Map<String, Object> cachedRow = new HashMap<String, Object>();
		
	public CassandraCrawlStatsUnit(Row row) {
		cachedRow.put(CassandraProperties.FIELD_CRAWL_STATS_CRAWL_ID, row.getString(CassandraProperties.FIELD_CRAWL_STATS_CRAWL_ID));
		cachedRow.put(CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP, row.getLong(CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP));
		cachedRow.put(CassandraProperties.FIELD_CRAWL_STATS_DOWNLOAD_VOLUME, row.getLong(CassandraProperties.FIELD_CRAWL_STATS_DOWNLOAD_VOLUME));
		cachedRow.put(CassandraProperties.FIELD_CRAWL_STATS_NUMBER_OF_URLS_CRAWLED, row.getLong(CassandraProperties.FIELD_CRAWL_STATS_NUMBER_OF_URLS_CRAWLED));
		cachedRow.put(CassandraProperties.FIELD_CRAWL_STATS_NEW_HOSTS_CRAWLED, row.getLong(CassandraProperties.FIELD_CRAWL_STATS_NEW_HOSTS_CRAWLED));
		cachedRow.put(CassandraProperties.FIELD_CRAWL_STATS_COMPLETED_HOSTS, row.getLong(CassandraProperties.FIELD_CRAWL_STATS_COMPLETED_HOSTS));
	}

	public String getCrawlID() {
		return (String) cachedRow.get(CassandraProperties.FIELD_CRAWL_STATS_CRAWL_ID);
	}
	
	@Override
	public long getTimestamp() {
		return (Long) cachedRow.get(CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP);
	}
	
	@Override
	public long getDownloadVolume() {
		return (Long) cachedRow.get(CassandraProperties.FIELD_CRAWL_STATS_DOWNLOAD_VOLUME);
	}
	
	public void setDownloadVolume(long volume) {
		cachedRow.put(CassandraProperties.FIELD_CRAWL_STATS_DOWNLOAD_VOLUME, volume);
	}
	
	@Override
	public long getNumberOfURLsCrawled() {
		return (Long) cachedRow.get(CassandraProperties.FIELD_CRAWL_STATS_NUMBER_OF_URLS_CRAWLED);
	}
	
	public void setNumberOfURLsCrawled(long urls) {
		cachedRow.put(CassandraProperties.FIELD_CRAWL_STATS_NUMBER_OF_URLS_CRAWLED, urls);
	}
	
	@Override
	public long getNumberOfNewHostsCrawled() {
		return (Long) cachedRow.get(CassandraProperties.FIELD_CRAWL_STATS_NEW_HOSTS_CRAWLED);
	}
	
	public void setNumberOfNewHostsCrawled(long crawled) {
		cachedRow.put(CassandraProperties.FIELD_CRAWL_STATS_NEW_HOSTS_CRAWLED, crawled);
	}
	
	@Override
	public long countCompletedHosts() {
		return (Long) cachedRow.get(CassandraProperties.FIELD_CRAWL_STATS_COMPLETED_HOSTS);
	}
	
	public void setCompletedHosts(long completed) {
		cachedRow.put(CassandraProperties.FIELD_CRAWL_STATS_COMPLETED_HOSTS, completed);
	}
	
	public void save(Session session) {
		String cql = "UPDATE " + TABLE + " SET ";
		for (Entry<String, Object> e : cachedRow.entrySet()) {
			if (!e.getKey().equals(CassandraProperties.FIELD_CRAWL_STATS_CRAWL_ID) && !e.getKey().equals(CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP)) {
				cql += e.getKey() + "=";
				if (e.getValue() instanceof String)
					cql += "'" + e.getValue() + "', ";
				else
					cql += e.getValue() + ", ";
			}
		}
				
		// Eliminate last comma
		cql = cql.substring(0, cql.length() - 2);
		
		cql +=	" WHERE " + CassandraProperties.FIELD_CRAWL_STATS_CRAWL_ID + "='" + getCrawlID() + "'" +
				" AND " + CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP + "=" + getTimestamp() + ";";		
		// Logger.info(cql);
		session.execute(cql);
	}
	
}
