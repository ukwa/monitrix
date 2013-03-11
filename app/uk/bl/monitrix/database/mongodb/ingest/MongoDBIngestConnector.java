package uk.bl.monitrix.database.mongodb.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import play.Logger;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.BasicDBObject;

import uk.bl.monitrix.database.DBIngestConnector;
import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.database.mongodb.model.MongoAlert;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlLogEntry;
import uk.bl.monitrix.database.mongodb.model.MongoIngestSchedule;
import uk.bl.monitrix.heritrix.LogFileEntry;
import uk.bl.monitrix.model.Alert;
import uk.bl.monitrix.model.IngestSchedule;

/**
 * An importer class that ingests a batch of crawl log entries, performing all necessary
 * data aggregation computations. 
 * 
 * IMPORTANT: the ingest process is not thread safe!
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoDBIngestConnector implements DBIngestConnector {
	
	// MongoDB host
	private Mongo mongo;

	// Monitrix database
	private DB db;
	
	// Ingest schedule
	private MongoIngestSchedule ingestSchedule;
	
	// Crawl log
	private MongoCrawlLogImporter crawlLogImporter;
	
	// Alert log
	private MongoAlertLogImporter alertLogImporter;
	
	// Known host list
	private MongoKnownHostImporter knownHostImporter;
	
	// Crawl stats
	private MongoCrawlStatsImporter crawlStatsImporter;
	
	public MongoDBIngestConnector() throws IOException {
		init(MongoProperties.DB_HOST, MongoProperties.DB_NAME, MongoProperties.DB_PORT);
	}
	
	public MongoDBIngestConnector(String hostName, String dbName, int dbPort) throws IOException {
		init(hostName, dbName, dbPort);
	}
	
	private void init(String hostName, String dbName, int dbPort) throws IOException {
		this.mongo = new Mongo(hostName, dbPort);
		this.db = mongo.getDB(dbName);

		this.ingestSchedule = new MongoIngestSchedule(db);
		this.crawlLogImporter = new MongoCrawlLogImporter(db);
		this.alertLogImporter = new MongoAlertLogImporter(db);
		this.knownHostImporter = new MongoKnownHostImporter(db, this.alertLogImporter);
		this.crawlStatsImporter = new MongoCrawlStatsImporter(db, knownHostImporter, new MongoVirusLogImporter(db));
	}
	
	@Override
	public IngestSchedule getIngestSchedule() {
		return ingestSchedule;
	}
	
	@Override
	public void insert(String logId, Iterator<LogFileEntry> iterator) {
		long start = System.currentTimeMillis();
		String crawlerId = ingestSchedule.getLog(logId).getCrawlerId();
		
		while (iterator.hasNext()) {
			long bulkStart = System.currentTimeMillis();
			
			List<MongoCrawlLogEntry> logEntryBatch = new ArrayList<MongoCrawlLogEntry>();
			List<MongoAlert> alertBatch = new ArrayList<MongoAlert>();
			
			int counter = 0; // Should be slightly faster than using list.size() to count
			long timeOfFirstLogEntryInBatch = Long.MAX_VALUE;
			while (iterator.hasNext() && (counter < MongoProperties.BULK_INSERT_CHUNK_SIZE)) {
				LogFileEntry next = iterator.next();
				counter++;
				
				long timestamp = next.getTimestamp().getTime();
				if (timestamp < timeOfFirstLogEntryInBatch)
					timeOfFirstLogEntryInBatch = timestamp;

				// Assemble MongoDB entity
				MongoCrawlLogEntry dbo = new MongoCrawlLogEntry(new BasicDBObject());
				dbo.setLogId(logId);
				dbo.setTimestamp(timestamp);
				dbo.setURL(next.getURL());
				dbo.setHost(next.getHost());
				dbo.setSubdomain(next.getSubdomain());
				dbo.setCrawlerID(next.getWorkerThread());
				dbo.setHTTPCode(next.getHTTPCode());
				dbo.setAnnotations(next.getAnnotations());
				dbo.setLogLine(next.toString());
				logEntryBatch.add(dbo);	
								
				// Update pre-aggregated stats
				crawlStatsImporter.update(next);
				
				// Host info
				knownHostImporter.addCrawlerID(next.getHost(), crawlerId);
				
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
			
			ingestSchedule.incrementIngestedLogLines(logId, counter);
			
			Logger.info("Done (" + (System.currentTimeMillis() - bulkStart) + " ms)");			
		}
				
		Logger.debug("Done - took " + (System.currentTimeMillis() - start) + " ms");		
	}
	
}
