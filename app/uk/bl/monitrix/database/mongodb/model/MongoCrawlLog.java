package uk.bl.monitrix.database.mongodb.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.CrawlLogEntry;
import uk.bl.monitrix.model.SearchResult;
import uk.bl.monitrix.model.SearchResultItem;

/**
 * A MongoDB-backed implementation of {@link CrawlLog}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 *
 */
public class MongoCrawlLog extends CrawlLog {
	
	protected DBCollection collection;
	
	public MongoCrawlLog(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_CRAWL_LOG);
		
		// The Heritrix Log collection is indexed by crawl log id, timestamp, url, hostname and annotations
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_LOG_ID, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_TIMESTAMP, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_URL, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_HOST, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_ANNOTATIONS_TOKENIZED, 1));
	}

	@Override
	public long getCrawlStartTime() {
		// TODO cache
		long crawlStartTime = 0;
		DBCursor cursor = collection.find().limit(1).sort(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_TIMESTAMP, 1));
		while (cursor.hasNext())
			crawlStartTime = new MongoCrawlLogEntry(cursor.next()).getTimestamp().getTime();					
		
		return crawlStartTime;
	}

	@Override
	public long getTimeOfLastCrawlActivity() {
		// TODO cache
		long lastCrawlActivity = 0;
		DBCursor cursor = collection.find().limit(1).sort(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_TIMESTAMP, -1));
		while (cursor.hasNext())
			lastCrawlActivity = new MongoCrawlLogEntry(cursor.next()).getTimestamp().getTime();					
		
		return lastCrawlActivity;
	}

	@Override
	public List<CrawlLogEntry> getMostRecentEntries(int n) {
		DBCursor cursor = collection.find().sort(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_TIMESTAMP, -1)).limit(n);
		
		List<CrawlLogEntry> recent = new ArrayList<CrawlLogEntry>();
		while(cursor.hasNext())
			recent.add(new MongoCrawlLogEntry(cursor.next()));

		return recent;
	}
	
	@Override
	public long countEntries() {
		return collection.count();
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public List<String> listLogIds() {
		return (List<String>) collection.distinct(MongoProperties.FIELD_CRAWL_LOG_LOG_ID);
	}

	@Override
	public long countEntriesForLog(String logId) {
		return collection.count(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_LOG_ID, logId));
	}
	
	@Override
	public List<CrawlLogEntry> getEntriesForURL(String url) {
		DBObject q = new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_URL, url);
		
		List<CrawlLogEntry> entries = new ArrayList<CrawlLogEntry>();
		DBCursor cursor = collection.find(q);
		while (cursor.hasNext())
			entries.add(new MongoCrawlLogEntry(cursor.next()));
		
		return entries;
	}

	@Override
	public SearchResult searchURLs(String query, int limit, int offset) {
		long startTime = System.currentTimeMillis();
		DBObject q = new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_URL, query);

		long total = collection.count(q);
		
		List<SearchResultItem> urls = new ArrayList<SearchResultItem>();
		DBCursor cursor = collection.find(q).skip(offset).limit(limit);
		while (cursor.hasNext()) {
			CrawlLogEntry entry = new MongoCrawlLogEntry(cursor.next());
			urls.add(new SearchResultItem(entry.getURL(), entry.toString()));
		}
	
		return new SearchResult(query, total, urls, limit, offset, System.currentTimeMillis() - startTime);
	}
	
	@Override
	public long countEntriesForHost(String hostname) {
		return collection.count(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_HOST, hostname));
	}

	@Override
	public Iterator<CrawlLogEntry> getEntriesForHost(String hostname) {
		long limit = collection.count(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_HOST, hostname));		
		
		// We're using a count first to improve performance (?)
		// Cf. http://docs.mongodb.org/manual/applications/optimization/
		final DBCursor cursor = collection
				.find(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_HOST, hostname))
				.hint(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_HOST, 1))
				.limit((int) limit);	
		
		return new Iterator<CrawlLogEntry>() {
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public CrawlLogEntry next() {
				return new MongoCrawlLogEntry(cursor.next());	
			}

			@Override
			public void remove() {
				cursor.remove();
			}
		};
	}

	@Override
	public long countEntriesWithAnnotation(String annotation) {
		DBObject q = new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_ANNOTATIONS_TOKENIZED, annotation); 
		return collection.count(q);
	}

	@Override
	public Iterator<CrawlLogEntry> getEntriesWithAnnotation(String annotation) {
		DBObject q = new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_ANNOTATIONS_TOKENIZED, annotation);
		final DBCursor cursor = collection.find(q);
		
		return new Iterator<CrawlLogEntry>() {
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public CrawlLogEntry next() {
				return new MongoCrawlLogEntry(cursor.next());
			}

			@Override
			public void remove() {
				cursor.remove();
			}
		};
	}

}
