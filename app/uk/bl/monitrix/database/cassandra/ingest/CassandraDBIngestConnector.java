package uk.bl.monitrix.database.cassandra.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import play.Logger;

import uk.bl.monitrix.database.DBIngestConnector;
import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.database.cassandra.model.CassandraAlert;
import uk.bl.monitrix.database.cassandra.model.CassandraCrawlLogEntry;
import uk.bl.monitrix.database.cassandra.model.CassandraIngestSchedule;
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
public class CassandraDBIngestConnector implements DBIngestConnector {
	
	// Ingest schedule
	private CassandraIngestSchedule ingestSchedule;
	
	// Crawl log
	private CassandraCrawlLogImporter crawlLogImporter;
	
	// Alert log
	private CassandraAlertLogImporter alertLogImporter;
	
	// Known host list
	private CassandraKnownHostImporter knownHostImporter;
	
	// Crawl stats
	private CassandraCrawlStatsImporter crawlStatsImporter;
	
	public CassandraDBIngestConnector() throws IOException {
//		init(CassandraProperties.DB_HOST, CassandraProperties.DB_NAME, CassandraProperties.DB_PORT);
	}
	
	public CassandraDBIngestConnector(String hostName, String dbName, int dbPort) throws IOException {
		init(hostName, dbName, dbPort);
	}
	
	private void init(String hostName, String dbName, int dbPort) throws IOException {
//		this.mongo = new Cassandra(hostName, dbPort);
//		this.db = mongo.getDB(dbName);
//
//		this.ingestSchedule = new CassandraIngestSchedule(db);
//		this.crawlLogImporter = new CassandraCrawlLogImporter(session);
//		this.alertLogImporter = new CassandraAlertLogImporter(db);
//		this.knownHostImporter = new CassandraKnownHostImporter(db, this.alertLogImporter);
//		this.crawlStatsImporter = new CassandraCrawlStatsImporter(db, knownHostImporter, new CassandraVirusLogImporter(db));
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
			
			List<CassandraCrawlLogEntry> logEntryBatch = new ArrayList<CassandraCrawlLogEntry>();
			List<CassandraAlert> alertBatch = new ArrayList<CassandraAlert>();
			
			int counter = 0; // Should be slightly faster than using list.size() to count
			long timeOfFirstLogEntryInBatch = Long.MAX_VALUE;
			while (iterator.hasNext() && (counter < CassandraProperties.BULK_INSERT_CHUNK_SIZE)) {
				LogFileEntry next = iterator.next();
				counter++;
				
				// Skip bad ones:
				if( next.getParseFailed() ) {
					Logger.error("Skipping storing a line due to a parse failure. "+counter);
					continue;
				}
				
				long timestamp = next.getLogTimestamp().getTime();
				if (timestamp < timeOfFirstLogEntryInBatch)
					timeOfFirstLogEntryInBatch = timestamp;

				// Assemble CassandraDB entity
//				CassandraCrawlLogEntry dbo = new CassandraCrawlLogEntry(new BasicDBObject());
//				dbo.setLogId(logId);
//				dbo.setTimestamp(timestamp);
//				dbo.setURL(next.getURL());
//				dbo.setHost(next.getHost());
//				dbo.setSubdomain(next.getSubdomain());
//				dbo.setCrawlerID(next.getWorkerThread());
//				dbo.setHTTPCode(next.getHTTPCode());
//				dbo.setAnnotations(next.getAnnotations());
//				dbo.setLogLine(next.toString());
//				dbo.setRetries(next.getRetries());
//				dbo.setCompressability(next.getCompressability());
//				logEntryBatch.add(dbo);	
								
				// Update pre-aggregated stats
				crawlStatsImporter.update(next);
				
				// Host info
				knownHostImporter.addCrawlerID(next.getHost(), crawlerId);
				
				// Log-entry-level alerts
				for (Alert a : next.getAlerts()) {
//					CassandraAlert alert = new CassandraAlert(new BasicDBObject());
//					alert.setTimestamp(next.getLogTimestamp().getTime());
//					alert.setOffendingHost(a.getOffendingHost());
//					alert.setAlertType(a.getAlertType());
//					alert.setAlertDescription(a.getAlertDescription());
//					alertBatch.add(alert);
				}
			}
			
			Logger.info("Processed " + counter + " log entries (" + (System.currentTimeMillis() - bulkStart) + " ms) - writing to DB");
			bulkStart = System.currentTimeMillis();
			
			//crawlLogImporter.insert(logEntryBatch);
			logEntryBatch.clear();
			
			//alertLogImporter.insert(alertBatch);
			alertBatch.clear();		
			
			crawlStatsImporter.commit();
			
			ingestSchedule.incrementIngestedLogLines(logId, counter);
			
			Logger.info("Done (" + (System.currentTimeMillis() - bulkStart) + " ms)");			
		}
				
		Logger.debug("Done - took " + (System.currentTimeMillis() - start) + " ms");		
	}
	
}
