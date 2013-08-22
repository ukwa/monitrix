package uk.bl.monitrix.database.cassandra.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import play.Logger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraDBConnector;
import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.CrawlLogEntry;
import uk.bl.monitrix.model.SearchResult;
import uk.bl.monitrix.model.SearchResultItem;

/**
 * A CassandraDB-backed implementation of {@link CrawlLog}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 *
 */
public class CassandraCrawlLog extends CrawlLog {
	
	protected Session session;
	
	public CassandraCrawlLog(Session session) {
		this.session = session;
	}
	
	protected Date getCoarseTimestamp( Date timestamp ) {
		return new Date(CassandraDBConnector.HOUR_AS_MILLIS*(timestamp.getTime()/CassandraDBConnector.HOUR_AS_MILLIS));
	}

	@Override
	public long getCrawlStartTime() {
		// TODO cache
		long crawlStartTime = 0;
		//DBCursor cursor = collection.find().limit(1).sort(new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_TIMESTAMP, 1));
		//while (cursor.hasNext())
		//	crawlStartTime = new CassandraCrawlLogEntry(cursor.next()).getLogTimestamp().getTime();					
		
		return crawlStartTime;
	}

	@Override
	public long getTimeOfLastCrawlActivity() {
		// TODO cache
		long lastCrawlActivity = 0;
		//DBCursor cursor = collection.find().limit(1).sort(new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_TIMESTAMP, -1));
		//while (cursor.hasNext())
		//	lastCrawlActivity = new CassandraCrawlLogEntry(cursor.next()).getLogTimestamp().getTime();					
		
		return lastCrawlActivity;
	}

	@Override
	public List<CrawlLogEntry> getMostRecentEntries(int n) {
		long startTime = System.currentTimeMillis();
		// Round the time down:
		Date coarse_ts = this.getCoarseTimestamp(new Date(startTime));
		// Search based on KEY, and range
		ResultSet results = session.execute("SELECT * FROM crawl_uris.log WHERE coarse_ts='"+coarse_ts.getTime()+"';");
		Iterator<Row> cursor = results.iterator();
		
		List<CrawlLogEntry> recent = new ArrayList<CrawlLogEntry>();
		while(cursor.hasNext())
			recent.add(new CassandraCrawlLogEntry(cursor.next()));

		return recent;
	}
	
	@Override
	public long countEntries() {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.crawls;");
		long grand_total = 0;
		Iterator<Row> rows = results.iterator();
		while( rows.hasNext() ) {
			grand_total += rows.next().getLong("total_urls");
		}
		return grand_total;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public List<String> listLogIds() {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.crawls;");
		List<String> collection = new ArrayList<String>();
		Iterator<Row> rows = results.iterator();
		while( rows.hasNext() ) {
			collection.add(rows.next().getString("crawl_id"));
		}
		return collection;
	}

	@Override
	public long countEntriesForLog(String logId) {
		ResultSet totalResults = session.execute("SELECT COUNT(*) FROM crawl_uris.log " +
		        "WHERE crawl_id = '"+logId+"';");
		return totalResults.one().getLong("count");
	}
	
	@Override
	public List<CrawlLogEntry> getEntriesForURL(String url) {
		Logger.info("Looking up "+url);
		
		ResultSet results = session.execute("SELECT * FROM crawl_uris.uris " +
		        "WHERE uri = '"+url+"';");
		
		Logger.info("Got "+results); 
		
		// Map from URI Table Results to Crawl Log Results		
		return getLogsFromUriRows(results);
	}
	
	private ResultSet getEntriesForTimestamp(Date timestamp) {
		Date coarse_ts = this.getCoarseTimestamp(timestamp);
		ResultSet results = session.execute("SELECT * FROM crawl_uris.log " +
		        "WHERE coarse_ts = '"+coarse_ts.getTime()+"' AND log_ts = '"+timestamp.getTime()+"' ;");
		return results;
	}
	private CrawlLogEntry getLogEntryForUriResult(String uri, Row ur) {
		ResultSet results = this.getEntriesForTimestamp(ur.getDate("log_ts"));
		Iterator<Row> rows = results.iterator();
		while( rows.hasNext() ) {
			Row r = rows.next();
			if(uri.equals(r.getString("uri")))
				return new CassandraCrawlLogEntry(r);
		}
		return null;
	}
	
	private List<CrawlLogEntry> getLogsFromUriRows( ResultSet results ) {
		List<CrawlLogEntry> entries = new ArrayList<CrawlLogEntry>();
		for( Row r : results.all() ) {
			entries.add(this.getLogEntryForUriResult(r.getString("uri"),r));
		}
		return entries;
	}

	// TODO eliminate code duplication
	@Override
	public SearchResult searchByURL(String query, int limit, int offset) {
		long startTime = System.currentTimeMillis();
		int off_limit = offset+limit;
		ResultSet results = session.execute("SELECT * FROM crawl_uris.uris " +
		        "WHERE uri = '"+query+"' LIMIT "+off_limit+";");
		
		List<CrawlLogEntry> entries = new ArrayList<CrawlLogEntry>();
		int i = 0;
		Iterator<Row> rows = results.iterator();
		while( rows.hasNext() ) {
			Row r = rows.next();
			if( i >= offset ) {
				entries.add( this.getLogEntryForUriResult(query,r) );
			}
			if( i == off_limit ) 
				break;
			i++;
		}
		
		ResultSet totalResults = session.execute("SELECT COUNT(*) FROM crawl_uris.uris " +
		        "WHERE uri = '"+query+"';");
		long total = totalResults.one().getLong("count");
		
		List<SearchResultItem> urls = new ArrayList<SearchResultItem>();
		for( CrawlLogEntry entry : entries ) {
			urls.add(new SearchResultItem(entry.getURL(), entry.toString()));
		}
	
		return new SearchResult(query, total, urls, limit, offset, System.currentTimeMillis() - startTime);
	}
	
	@Override
	public SearchResult searchByAnnotation(String annotation, int limit, int offset) {
		long startTime = System.currentTimeMillis();
		//DBObject q = new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_ANNOTATIONS_TOKENIZED, annotation);
		
		long total = 0;//collection.count(q);
		
		List<SearchResultItem> urls = new ArrayList<SearchResultItem>();
		//DBCursor cursor = collection.find(q).skip(offset).limit(limit);
		//while (cursor.hasNext()) {
		//	CrawlLogEntry entry = new CassandraCrawlLogEntry(cursor.next());
		//	urls.add(new SearchResultItem(entry.getURL(), entry.toString()));
		//}
		
		return new SearchResult(annotation, total, urls, limit, offset, System.currentTimeMillis() - startTime);
	}
	
	@Override
	public SearchResult searchByCompressability(double from, double to, int limit, int offset) {
		Logger.debug("Searching by compressability");
		long startTime = System.currentTimeMillis();
		
		//DBObject query = new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_COMPRESSABILITY, 
		//		new BasicDBObject("$gte", from).append("$lt", to));
		
		long total = 0; //collection.count(query);
		
		List<SearchResultItem> urls = new ArrayList<SearchResultItem>();
		
//		if (limit > 0) {
//			DBCursor cursor = collection.find(query).skip(offset).limit(limit);
//			while (cursor.hasNext()) {
//				CrawlLogEntry entry = new CassandraCrawlLogEntry(cursor.next());
//				urls.add(new SearchResultItem(entry.getURL(), entry.toString()));
//			}
//		}
//		
		Logger.debug("Done - took " + (System.currentTimeMillis() - startTime));
		return new SearchResult(null, total, urls, limit, offset, System.currentTimeMillis() - startTime);
	}
	
	@Override
	public long countEntriesForHost(String hostname) {
		ResultSet totalResults = session.execute("SELECT COUNT(*) FROM crawl_uris.log " +
		        "WHERE host = '"+hostname+"';");
		return totalResults.one().getLong("count");
	}

	@Override
	public Iterator<CrawlLogEntry> getEntriesForHost(String hostname) {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.log " +
		        "WHERE host='"+hostname+"';");

		final Iterator<Row> cursor = results.iterator();
		
		return new Iterator<CrawlLogEntry>() {
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public CrawlLogEntry next() {
				return new CassandraCrawlLogEntry(cursor.next());	
			}

			@Override
			public void remove() {
				cursor.remove();
			}
		};
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<String> extractHostsForAnnotation(String annotation) {
		List<String> hosts = new ArrayList<String>();
		return hosts;
		//DBObject q = new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_ANNOTATIONS_TOKENIZED, annotation);
		//return (List<String>) collection.distinct(CassandraProperties.FIELD_CRAWL_LOG_HOST, q);
	}

}
