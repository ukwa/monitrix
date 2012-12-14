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
import uk.bl.monitrix.stats.CrawlStatistics;

/**
 * An implementation of {@link DBConnector} for MongoDB.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoConnector implements DBConnector {
	
	// String constants - configuration properties
	private static final String PROP_KEY_MONGO_SERVER = "mongo.host";
	private static final String PROP_KEY_MONGO_PORT = "mongo.port";
	private static final String PROP_KEY_DB_NAME = "mongo.db.name";
	
	// String constants - DB collection names
	static final String COLLECTION_NAME_GLOBAL_STATS = "global-stats";
	static final String COLLECTION_NAME_LOG = "heretrix-log";
	
	// String constants - DB column/field keys
	static final String FIELD_TIMESTAMP = "timestamp";
	static final String FIELD_LOG_LINE = "line";
	static final String FIELD_NUMBER_OF_LINES_TOTAL = "lines-total";
	static final String FIELD_CRAWL_START = "crawl-started";
	static final String FIELD_CRAWL_LAST_ACTIVITIY = "crawl-last-activity";
	
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
		String dbHost = config.getString(PROP_KEY_MONGO_SERVER);
		String dbName = config.getString(PROP_KEY_DB_NAME);
		int dbPort;
		try {
			dbPort = Integer.parseInt(config.getString(PROP_KEY_MONGO_PORT));
		} catch (Throwable t) {
			Logger.warn("Error reading mongo.port from application.conf - defaulting to 27017");
			dbPort = 27017;
		}
		init(dbHost, dbName, dbPort);
	}
	
	public MongoConnector(String hostName, String dbName, int dbPort) throws IOException {
		init(hostName, dbName, dbPort);
	}
	
	private void init(String hostName, String dbName, int dbPort) throws IOException {
		this.mongo = new Mongo(hostName, dbPort);
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
		
		// Keep track of global statistics
		long numberOfLines = 0;
		long crawlStart = Long.MAX_VALUE;
		long crawlLastActivity = 0;
		
		while (iterator.hasNext()) {
			long bulkStart = System.currentTimeMillis();
			
			List<DBObject> bulk = new ArrayList<DBObject>();
			
			int counter = 0; // Should be slightly faster than using list size
			while (iterator.hasNext() & counter < BULK_SIZE) {
				LogEntry next = iterator.next();
				
				long timestamp = next.getTimestamp().getTime();
				if (timestamp < crawlStart)
					crawlStart = timestamp;
				if (timestamp > crawlLastActivity)
					crawlLastActivity = timestamp;

				BasicDBObject dbo = new BasicDBObject();
				dbo.put(FIELD_TIMESTAMP, timestamp);
				dbo.put(FIELD_LOG_LINE, next.toString());
				bulk.add(dbo);	
				
				counter++;
			}
			numberOfLines += counter;
			
			log.insert(bulk);
			Logger.info("Wrote " + counter + " log entries to MongoDB - took " + (System.currentTimeMillis() - bulkStart) + " ms");
		}
		
		// TODO update rather than replace!
		BasicDBObject insertStats = new BasicDBObject();
		insertStats.put(FIELD_NUMBER_OF_LINES_TOTAL, numberOfLines);
		insertStats.put(FIELD_CRAWL_START, crawlStart);
		insertStats.put(FIELD_CRAWL_LAST_ACTIVITIY, crawlLastActivity);
		globalStats.drop();
		globalStats.insert(insertStats);
		
		Logger.info("Done - took " + (System.currentTimeMillis() - start) + " ms");
	}
	
	@Override
	public CrawlStatistics getCrawlStatistics() {
		return new MongoBackedCrawlStatistics(db);
	}

	@Override
	public void close() {
		this.mongo.close();
	}

}
