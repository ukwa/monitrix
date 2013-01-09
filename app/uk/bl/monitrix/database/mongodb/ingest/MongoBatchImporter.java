package uk.bl.monitrix.database.mongodb.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import play.Logger;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.BasicDBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlLogEntry;
import uk.bl.monitrix.heritrix.LogFileEntry;

public class MongoBatchImporter {
	
	// MongoDB host
	private Mongo mongo;

	// Monitrix database
	private DB db;
	
	// Crawl stats
	private MongoCrawlStatsImporter crawlStatsImporter;
	
	// Crawl log
	private MongoCrawlLogImporter crawlLogImporter;
	
	public MongoBatchImporter() throws IOException {
		init(MongoProperties.DB_HOST, MongoProperties.DB_NAME, MongoProperties.DB_PORT);
	}
	
	public MongoBatchImporter(String hostName, String dbName, int dbPort) throws IOException {
		init(hostName, dbName, dbPort);
	}
	
	private void init(String hostName, String dbName, int dbPort) throws IOException {
		this.mongo = new Mongo(hostName, dbPort);
		this.db = mongo.getDB(dbName);
		
		this.crawlStatsImporter = new MongoCrawlStatsImporter(db, new MongoKnownHostImporter(db));
		this.crawlLogImporter = new MongoCrawlLogImporter(db);
	}
	
	public void insert(Iterator<LogFileEntry> iterator) {
		Logger.info("Writing log to MongoDB");
		long start = System.currentTimeMillis();
		
		while (iterator.hasNext()) {
			long bulkStart = System.currentTimeMillis();
			
			List<MongoCrawlLogEntry> bulk = new ArrayList<MongoCrawlLogEntry>();
			
			int counter = 0; // Should be slightly faster than using list.size() to count
			while (iterator.hasNext() & counter < MongoProperties.BULK_INSERT_CHUNK_SIZE) {
				LogFileEntry next = iterator.next();
				counter++;

				// Assemble MongoDB entity
				MongoCrawlLogEntry dbo = new MongoCrawlLogEntry(new BasicDBObject());
				dbo.setTimestamp(next.getTimestamp().getTime());
				dbo.setHost(next.getHost());
				dbo.setCrawlerID(next.getCrawlerID());
				dbo.setHTTPCode(next.getHTTPCode());
				dbo.setLogLine(next.toString());
				bulk.add(dbo);	
				
				// Update pre-aggregated stats
				crawlStatsImporter.update(next);
				
				// TODO record log-entry-level alerts
			}

			Logger.info("Processed " + counter + " log entries (" + (System.currentTimeMillis() - bulkStart) + " ms) - writing to DB");
			bulkStart = System.currentTimeMillis();
			crawlLogImporter.insert(bulk);
			Logger.info("Done (" + (System.currentTimeMillis() - bulkStart) + " ms)");			
		}
		crawlStatsImporter.commit();
				
		Logger.info("Done - took " + (System.currentTimeMillis() - start) + " ms");		
	}
	
}
