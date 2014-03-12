package uk.bl.monitrix.database.cassandra.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import uk.bl.monitrix.model.CrawlStats;
import uk.bl.monitrix.model.CrawlStatsUnit;
import uk.bl.monitrix.database.cassandra.CassandraProperties;

/**
 * A CassandraDB-backed implementation of {@link CrawlStats}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraCrawlStats implements CrawlStats {
	
	private final String TABLE_STATS = CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_CRAWL_STATS;
	
	protected Session session; 
	
	// A simple in-memory buffer for quick stats lookups
	protected Map<Long, CassandraCrawlStatsUnit> cache = new HashMap<Long, CassandraCrawlStatsUnit>();
	
	public CassandraCrawlStats(Session session) {
		this.session = session;
	}
	
	@Override
	public Iterator<CrawlStatsUnit> getCrawlStats() {
		// TODO conflate stats from different crawls
		final Iterator<Row> cursor =
				session.execute("SELECT * FROM " + TABLE_STATS + " WHERE " + CassandraProperties.FIELD_CRAWL_STATS_CRAWL_ID +
		        " IN ('file*') ORDER BY " + CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP + ";")
				.iterator();
		
		return new Iterator<CrawlStatsUnit>() {
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public CrawlStatsUnit next() {
				return new CassandraCrawlStatsUnit(cursor.next());
			}

			@Override
			public void remove() {
				cursor.remove();	
			}
		};
	}

	@Override
	public Iterator<CrawlStatsUnit> getCrawlStats(String crawl_id) {
		final Iterator<Row> cursor =
				session.execute("SELECT * FROM " + TABLE_STATS + " WHERE " + CassandraProperties.FIELD_CRAWL_STATS_CRAWL_ID + "='" +
		        crawl_id + "' ORDER BY " + CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP + ";")
				.iterator();
		
		return new Iterator<CrawlStatsUnit>() {
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public CrawlStatsUnit next() {
				return new CassandraCrawlStatsUnit(cursor.next());
			}

			@Override
			public void remove() {
				cursor.remove();	
			}
		};
	}

	@Override
	public CrawlStatsUnit getStatsForTimestamp(long timestamp, String crawl_id) {
		// TODO conflate stats from different crawls
		if (cache.containsKey(timestamp))
			return cache.get(timestamp);
		
		ResultSet results = session.execute("SELECT * FROM " + TABLE_STATS + " WHERE " + CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP +
				"=" + timestamp + " AND " + CassandraProperties.FIELD_CRAWL_STATS_CRAWL_ID + "='" + crawl_id + "' ORDER BY " + 
				CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP + ";");
		
		if (results.isExhausted() ) {
			return null;
		} else {
			CassandraCrawlStatsUnit stats = new CassandraCrawlStatsUnit(results.one());
			cache.put(timestamp, stats);
			return stats;
		}
	}

	@Override
	public List<CrawlStatsUnit> getMostRecentStats(int n) {
		// TODO conflate stats from different crawls
		Iterator<Row> cursor = 
				session.execute("SELECT * FROM " + TABLE_STATS + " ORDER BY " + 
		        CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP + " LIMIT " + n +";").iterator();
		
		List<CrawlStatsUnit> recent = new ArrayList<CrawlStatsUnit>();
		while(cursor.hasNext())
			recent.add(new CassandraCrawlStatsUnit(cursor.next()));

		return recent;
	}

}
