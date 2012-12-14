package uk.bl.monitrix.db.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import play.Configuration;
import play.Logger;
import play.Play;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import uk.bl.monitrix.db.DBConnector;
import uk.bl.monitrix.heritrix.LogEntry;

/**
 * An implementation of {@link DBConnector} for MongoDB.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoConnector implements DBConnector {
	
	// String constants - configuration properties
	private static final String PROP_KEY_MONGO_SERVER = "mongo.host";
	private static final String PROP_KEY_DB_NAME = "mongo.db.name";
	
	// String constants - DB collection names
	private static final String COLLECTION_NAME_GLOBAL_STATS = "global-stats";
	private static final String COLLECTION_NAME_LOG = "heretrix-log";
	
	// String constants - DB column/field keys
	private static final String FIELD_TIMESTAMP = "timestamp";
	private static final String FIELD_LOG_LINE = "line";
	
	// Bulk insert chunk size
	private static final int BULK_SIZE = 500000;
	
	// Mongo DB host
	private Mongo mongo;
	
	// Monitrix DB
	private DB db;
	
	// DB collection containing global stats
	private DBCollection globalStats;
	
	// DB collection containing the raw log file
	private DBCollection log;
	
	public MongoConnector() throws IOException {
		Configuration config = Play.application().configuration();
		init(config.getString(PROP_KEY_MONGO_SERVER), config.getString(PROP_KEY_DB_NAME));
	}
	
	public MongoConnector(String hostName, String dbName) throws IOException {
		init(hostName, dbName);
	}
	
	private void init(String hostName, String dbName) throws IOException {
		this.mongo = new Mongo(hostName);
		this.db = mongo.getDB(dbName);
		this.globalStats = db.getCollection(COLLECTION_NAME_GLOBAL_STATS);
		this.log = db.getCollection(COLLECTION_NAME_LOG);
		
		// Index log collection by timestamp (will be skipped by Mongo if index exists)
		this.log.createIndex(new BasicDBObject("timestamp", 1));		
	}

	@Override
	public void insert(Iterator<LogEntry> iterator) {
		Logger.info("Writing log to MongoDB");
		long start = System.currentTimeMillis();
		
		while (iterator.hasNext()) {
			long bulkStart = System.currentTimeMillis();
			
			List<DBObject> bulk = new ArrayList<DBObject>();
			
			int counter = 0; // Should be slightly faster than using list size
			while (iterator.hasNext() & counter < BULK_SIZE) {
				LogEntry next = iterator.next();
				
				BasicDBObject dbo = new BasicDBObject();
				dbo.put(FIELD_TIMESTAMP, next.getTimestamp().getTime());
				dbo.put(FIELD_LOG_LINE, next.toString());
				bulk.add(dbo);	
				
				counter++;
			}
			
			log.insert(bulk);
			Logger.info("Wrote " + counter + " log entries to MongoDB - took " + (System.currentTimeMillis() - bulkStart) + " ms");
		}
		
		Logger.info("Done - took " + (System.currentTimeMillis() - start) + " ms");
	}

	@Override
	public void close() {
		this.mongo.close();
	}

}
