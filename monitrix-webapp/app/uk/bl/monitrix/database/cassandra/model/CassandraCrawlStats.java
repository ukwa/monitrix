package uk.bl.monitrix.database.cassandra.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;

import play.Logger;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import uk.bl.monitrix.model.CrawlStats;
import uk.bl.monitrix.model.CrawlStatsUnit;
import uk.bl.monitrix.model.IngestSchedule;
import uk.bl.monitrix.model.IngestedLog;
import uk.bl.monitrix.database.cassandra.CassandraProperties;

/**
 * A CassandraDB-backed implementation of {@link CrawlStats}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraCrawlStats implements CrawlStats {
	
	private final String TABLE_STATS = CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_CRAWL_STATS;
	
	protected Session session;
	
	private IngestSchedule ingestSchedule;
	
	// A simple in-memory buffer for quick stats lookups
	protected Map<Long, CassandraCrawlStatsUnit> cache = new HashMap<Long, CassandraCrawlStatsUnit>();
	
	public CassandraCrawlStats(Session session, IngestSchedule ingestSchedule) {
		this.session = session;
		this.ingestSchedule = ingestSchedule;
	}
	
	@Override
	public Iterator<CrawlStatsUnit> getCrawlStats() {
		Logger.info("Getting crawl stats");		
		
		// This is ridiculous
		List<String> logs = new ArrayList<String>();
		for (IngestedLog l : ingestSchedule.getLogs()) {
			logs.add(l.getId());
		}
		
		final Iterator<Row> cursor =
				session.execute("SELECT * FROM " + TABLE_STATS + " WHERE " + CassandraProperties.FIELD_CRAWL_STATS_CRAWL_ID +
		        " IN ('" + StringUtils.join(logs, ",") + "') ORDER BY " + CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP + ";")
				.iterator();
		
		/*
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
		*/
		
		// This re-implementation of Scala's .groupBy function is also quite sad :-(
		Map<Long, List<CrawlStatsUnit>> groupedByTimestamp = new TreeMap<Long, List<CrawlStatsUnit>>();
		while (cursor.hasNext()) {
			CrawlStatsUnit u = new CassandraCrawlStatsUnit(cursor.next());
			List<CrawlStatsUnit> unitsFromIndividualCrawls = groupedByTimestamp.get(u.getTimestamp());
			if (unitsFromIndividualCrawls == null)
				unitsFromIndividualCrawls = new ArrayList<CrawlStatsUnit>();
			
			unitsFromIndividualCrawls.add(u);
			groupedByTimestamp.put(u.getTimestamp(), unitsFromIndividualCrawls);
		}
		
		List<CrawlStatsUnit> conflated = new ArrayList<CrawlStatsUnit>();
		for (final Entry<Long, List<CrawlStatsUnit>> entry : groupedByTimestamp.entrySet()) {
			final List<CrawlStatsUnit> units = entry.getValue();
			conflated.add(new CrawlStatsUnit() {
				@Override
				public long getTimestamp() {
					return entry.getKey();
				}
				
				@Override
				public long getNumberOfURLsCrawled() {
					long urls = 0;

					for (CrawlStatsUnit u: units)
						urls += u.getNumberOfURLsCrawled();
					
					return urls;
				}
				
				@Override
				public long getNumberOfNewHostsCrawled() {
					long hosts = 0;

					for (CrawlStatsUnit u: units)
						hosts += u.getNumberOfNewHostsCrawled();
					
					return hosts;
				}
				
				@Override
				public long getDownloadVolume() {
					long volume = 0;

					for (CrawlStatsUnit u: units)
						volume += u.getDownloadVolume();
					
					return volume;
				}
				
				@Override
				public long countCompletedHosts() {
					long hosts = 0;

					for (CrawlStatsUnit u: units)
						hosts += u.countCompletedHosts();
					
					return hosts;
				}
			});
		}
		
		return conflated.iterator();
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
