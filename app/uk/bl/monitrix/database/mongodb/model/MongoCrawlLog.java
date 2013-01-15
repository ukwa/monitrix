package uk.bl.monitrix.database.mongodb.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.CrawlLogEntry;

/**
 * A MongoDB-backed implementation of {@link CrawlLog}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 *
 */
public class MongoCrawlLog extends CrawlLog {
	
	protected DBCollection collection;
	
	public MongoCrawlLog(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_CRAWL_LOG);
		
		// The Heritrix Log collection is indexed by timestamp and hostname (will be skipped automatically if index exists)
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_TIMESTAMP, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_HOST, 1));
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
	public long countEntriesForHost(String hostname) {
		return collection.count(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_HOST, hostname));
	}

	@Override
	public Iterator<CrawlLogEntry> getEntriesForHost(String hostname) {
		final DBCursor cursor = collection.find(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_HOST, hostname));
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
