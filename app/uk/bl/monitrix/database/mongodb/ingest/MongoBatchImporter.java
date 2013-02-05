package uk.bl.monitrix.database.mongodb.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import play.Logger;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.BasicDBObject;

import uk.bl.monitrix.database.DBBatchImporter;
import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.database.mongodb.model.MongoAlert;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlLogEntry;
import uk.bl.monitrix.heritrix.LogFileEntry;
import uk.bl.monitrix.model.Alert;

/**
 * An importer class that ingests a batch of crawl log entries, performing all necessary
 * data aggregation computations. 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoBatchImporter implements DBBatchImporter {
	
	// MongoDB host
	private Mongo mongo;

	// Monitrix database
	private DB db;
	
	// Crawl log
	private MongoCrawlLogImporter crawlLogImporter;
	
	// Alert log
	private MongoAlertLogImporter alertLogImporter;
	
	// Crawl stats
	private MongoCrawlStatsImporter crawlStatsImporter;
	
	public MongoBatchImporter() throws IOException {
		init(MongoProperties.DB_HOST, MongoProperties.DB_NAME, MongoProperties.DB_PORT);
	}
	
	public MongoBatchImporter(String hostName, String dbName, int dbPort) throws IOException {
		init(hostName, dbName, dbPort);
	}
	
	private void init(String hostName, String dbName, int dbPort) throws IOException {
		this.mongo = new Mongo(hostName, dbPort);
		this.db = mongo.getDB(dbName);

		this.crawlLogImporter = new MongoCrawlLogImporter(db);
		this.alertLogImporter = new MongoAlertLogImporter(db);
		this.crawlStatsImporter = new MongoCrawlStatsImporter(db,  new MongoKnownHostImporter(db, this.alertLogImporter), new MongoVirusLogImporter(db));
	}
	
	@Override
	public long countEntriesForCrawler(String logPath) {
		return crawlLogImporter.countEntriesForCrawler(logPath);
	}
	
	@Override
	public void insert(String logPath, Iterator<LogFileEntry> iterator) {
		Logger.info("Writing log to MongoDB");
		long start = System.currentTimeMillis();
		
		while (iterator.hasNext()) {
			long bulkStart = System.currentTimeMillis();
			
			List<MongoCrawlLogEntry> logEntryBatch = new ArrayList<MongoCrawlLogEntry>();
			List<MongoAlert> alertBatch = new ArrayList<MongoAlert>();
			
			int counter = 0; // Should be slightly faster than using list.size() to count
			long timeOfFirstLogEntryInBatch = Long.MAX_VALUE;
			while (iterator.hasNext() & counter < MongoProperties.BULK_INSERT_CHUNK_SIZE) {
				LogFileEntry next = iterator.next();
				counter++;
				
				long timestamp = next.getTimestamp().getTime();
				if (timestamp < timeOfFirstLogEntryInBatch)
					timeOfFirstLogEntryInBatch = timestamp;

				// Assemble MongoDB entity
				MongoCrawlLogEntry dbo = new MongoCrawlLogEntry(new BasicDBObject());
				dbo.setLogPath(logPath);
				dbo.setTimestamp(timestamp);
				dbo.setHost(next.getHost());
				dbo.setSubdomain(next.getSubdomain());
				dbo.setCrawlerID(next.getCrawlerID());
				dbo.setHTTPCode(next.getHTTPCode());
				dbo.setAnnotations(next.getAnnotations());
				dbo.setLogLine(next.toString());
				logEntryBatch.add(dbo);	
								
				// Update pre-aggregated stats
				crawlStatsImporter.update(next);
				
				// Log-entry-level alerts
				for (Alert a : next.getAlerts()) {
					MongoAlert alert = new MongoAlert(new BasicDBObject());
					alert.setTimestamp(next.getTimestamp().getTime());
					alert.setOffendingHost(a.getOffendingHost());
					alert.setAlertType(a.getAlertType());
					alert.setAlertDescription(a.getAlertDescription());
					alertBatch.add(alert);
				}
			}

			Logger.info("Processed " + counter + " log entries (" + (System.currentTimeMillis() - bulkStart) + " ms) - writing to DB");
			bulkStart = System.currentTimeMillis();
			
			crawlLogImporter.insert(logEntryBatch);
			logEntryBatch.clear();
			
			alertLogImporter.insert(alertBatch);
			alertBatch.clear();		
			
			crawlStatsImporter.commit();
			Logger.info("Done (" + (System.currentTimeMillis() - bulkStart) + " ms)");			
		}
				
		Logger.info("Done - took " + (System.currentTimeMillis() - start) + " ms");		
	}
	
}
