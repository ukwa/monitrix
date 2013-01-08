package uk.bl.monitrix.db.mongodb.model.crawllog;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

/**
 * Wraps the MongoDB 'Heritrix Log' collection.
 * 
 * TODO caching for most recent log entries!
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CrawlLogCollection {
		
	private DBCollection collection;
	
	public CrawlLogCollection(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_CRAWL_LOG);
		
		// The Heritrix Log collection is indexed by timestamp and hostname (will be skipped automatically if index exists)
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_LOG_TIMESTAMP, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_LOG_HOST, 1));
	}
	
	/**
	 * Inserts a list of log entries into the collection.
	 * @param log the log entries
	 */
	public void insert(List<CrawlLogDBO> log) {
		collection.insert(CrawlLogDBO.map(log));
	}

	/**
	 * Counts the log entries for a specific host.
	 * @param hostname the host name
	 * @return the number of log entries for the host
	 */
	public long countEntriesForHost(String hostname) {
		return collection.count(new BasicDBObject(MongoProperties.FIELD_LOG_HOST, hostname));
	}
	
	/**
	 * Returns the log entries for a specific host.
	 * @param hostname the host name
	 * @return the log entries for the host
	 */
	public Iterator<CrawlLogDBO> getEntriesForHost(String hostname) {
		final DBCursor cursor = collection.find(new BasicDBObject(MongoProperties.FIELD_LOG_HOST, hostname));

		return new Iterator<CrawlLogDBO>() {
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public CrawlLogDBO next() {
				return new CrawlLogDBO(cursor.next());	
			}

			@Override
			public void remove() {
				cursor.remove();
			}
		};
	}
	
	/**
	 * Returns the N most recent log entries. 
	 * @param n the number of log entries to return
	 * @return the log entries
	 */
	public List<CrawlLogDBO> getMostRecentEntries(int n) {
		DBCursor cursor = collection.find().sort(new BasicDBObject(MongoProperties.FIELD_LOG_TIMESTAMP, -1)).limit(n);
			
		List<CrawlLogDBO> recent = new ArrayList<CrawlLogDBO>();
		while(cursor.hasNext())
			recent.add(new CrawlLogDBO(cursor.next()));

		return recent;
	}

}
