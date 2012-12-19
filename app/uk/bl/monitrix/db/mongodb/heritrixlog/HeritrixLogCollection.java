package uk.bl.monitrix.db.mongodb.heritrixlog;

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
public class HeritrixLogCollection {
		
	private DBCollection collection;
	
	public HeritrixLogCollection(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_HERITRIX_LOG);
		
		// The Heritrix Log collection is indexed by timestamp and hostname (will be skipped automatically if index exists)
		this.collection.createIndex(new BasicDBObject(MongoProperties.FIELD_LOG_TIMESTAMP, 1));
		this.collection.createIndex(new BasicDBObject(MongoProperties.FIELD_LOG_HOST, 1));
	}
	
	/**
	 * Inserts a list of log entries into the collection.
	 * @param log the log entries
	 */
	public void insert(List<HeritrixLogDBO> log) {
		collection.insert(HeritrixLogDBO.map(log));
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
	public Iterator<HeritrixLogDBO> getEntriesForHost(String hostname) {
		final DBCursor cursor = collection.find(new BasicDBObject(MongoProperties.FIELD_LOG_HOST, hostname));

		return new Iterator<HeritrixLogDBO>() {
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public HeritrixLogDBO next() {
				return new HeritrixLogDBO(cursor.next());	
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
	public List<HeritrixLogDBO> getMostRecentEntries(int n) {
		DBCursor cursor = collection.find().sort(new BasicDBObject(MongoProperties.FIELD_LOG_TIMESTAMP, -1)).limit(n);
			
		List<HeritrixLogDBO> recent = new ArrayList<HeritrixLogDBO>();
		while(cursor.hasNext())
			recent.add(new HeritrixLogDBO(cursor.next()));

		return recent;
	}

}
