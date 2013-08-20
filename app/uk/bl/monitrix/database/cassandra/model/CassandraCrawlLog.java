package uk.bl.monitrix.database.cassandra.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import play.Logger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

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

	@Override
	public long getCrawlStartTime() {
		// TODO cache
		long crawlStartTime = 0;
		DBCursor cursor = collection.find().limit(1).sort(new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_TIMESTAMP, 1));
		while (cursor.hasNext())
			crawlStartTime = new CassandraCrawlLogEntry(cursor.next()).getLogTimestamp().getTime();					
		
		return crawlStartTime;
	}

	@Override
	public long getTimeOfLastCrawlActivity() {
		// TODO cache
		long lastCrawlActivity = 0;
		DBCursor cursor = collection.find().limit(1).sort(new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_TIMESTAMP, -1));
		while (cursor.hasNext())
			lastCrawlActivity = new CassandraCrawlLogEntry(cursor.next()).getLogTimestamp().getTime();					
		
		return lastCrawlActivity;
	}

	@Override
	public List<CrawlLogEntry> getMostRecentEntries(int n) {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.log LIMIT 1;");
		DBCursor cursor = collection.find().sort(new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_TIMESTAMP, -1)).limit(n);
		
		List<CrawlLogEntry> recent = new ArrayList<CrawlLogEntry>();
		while(cursor.hasNext())
			recent.add(new CassandraCrawlLogEntry(cursor.next()));

		return recent;
	}
	
	@Override
	public long countEntries() {
		return collection.count();
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public List<String> listLogIds() {
		return (List<String>) collection.distinct(CassandraProperties.FIELD_CRAWL_LOG_LOG_ID);
	}

	@Override
	public long countEntriesForLog(String logId) {
		return collection.count(new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_LOG_ID, logId));
	}
	
	@Override
	public List<CrawlLogEntry> getEntriesForURL(String url) {
		DBObject q = new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_URL, url);
		
		List<CrawlLogEntry> entries = new ArrayList<CrawlLogEntry>();
		DBCursor cursor = collection.find(q);
		while (cursor.hasNext())
			entries.add(new CassandraCrawlLogEntry(cursor.next()));
		
		return entries;
	}

	// TODO eliminate code duplication
	@Override
	public SearchResult searchByURL(String query, int limit, int offset) {
		long startTime = System.currentTimeMillis();
		DBObject q = new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_URL, query);

		long total = collection.count(q);
		
		List<SearchResultItem> urls = new ArrayList<SearchResultItem>();
		DBCursor cursor = collection.find(q).skip(offset).limit(limit);
		while (cursor.hasNext()) {
			CrawlLogEntry entry = new CassandraCrawlLogEntry(cursor.next());
			urls.add(new SearchResultItem(entry.getURL(), entry.toString()));
		}
	
		return new SearchResult(query, total, urls, limit, offset, System.currentTimeMillis() - startTime);
	}
	
	@Override
	public SearchResult searchByAnnotation(String annotation, int limit, int offset) {
		long startTime = System.currentTimeMillis();
		DBObject q = new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_ANNOTATIONS_TOKENIZED, annotation);
		
		long total = collection.count(q);
		
		List<SearchResultItem> urls = new ArrayList<SearchResultItem>();
		DBCursor cursor = collection.find(q).skip(offset).limit(limit);
		while (cursor.hasNext()) {
			CrawlLogEntry entry = new CassandraCrawlLogEntry(cursor.next());
			urls.add(new SearchResultItem(entry.getURL(), entry.toString()));
		}
		
		return new SearchResult(annotation, total, urls, limit, offset, System.currentTimeMillis() - startTime);
	}
	
	@Override
	public SearchResult searchByCompressability(double from, double to, int limit, int offset) {
		Logger.debug("Searching by compressability");
		long startTime = System.currentTimeMillis();
		
		DBObject query = new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_COMPRESSABILITY, 
				new BasicDBObject("$gte", from).append("$lt", to));
		
		long total = collection.count(query);
		
		List<SearchResultItem> urls = new ArrayList<SearchResultItem>();
		
		if (limit > 0) {
			DBCursor cursor = collection.find(query).skip(offset).limit(limit);
			while (cursor.hasNext()) {
				CrawlLogEntry entry = new CassandraCrawlLogEntry(cursor.next());
				urls.add(new SearchResultItem(entry.getURL(), entry.toString()));
			}
		}
		
		Logger.debug("Done - took " + (System.currentTimeMillis() - startTime));
		return new SearchResult(null, total, urls, limit, offset, System.currentTimeMillis() - startTime);
	}
	
	@Override
	public long countEntriesForHost(String hostname) {
		return collection.count(new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_HOST, hostname));
	}

	@Override
	public Iterator<CrawlLogEntry> getEntriesForHost(String hostname) {
		long limit = collection.count(new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_HOST, hostname));		
		
		// We're using a count first to improve performance (?)
		// Cf. http://docs.mongodb.org/manual/applications/optimization/
		final DBCursor cursor = collection
				.find(new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_HOST, hostname))
				.hint(new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_HOST, 1))
				.limit((int) limit);	
		
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
		DBObject q = new BasicDBObject(CassandraProperties.FIELD_CRAWL_LOG_ANNOTATIONS_TOKENIZED, annotation);
		return (List<String>) collection.distinct(CassandraProperties.FIELD_CRAWL_LOG_HOST, q);
	}

}
