package uk.bl.monitrix.database.cassandra.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import play.Logger;
import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.database.DBIngestConnector;
import uk.bl.monitrix.database.cassandra.CassandraDBConnector;
import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.database.cassandra.model.CassandraIngestSchedule;
import uk.bl.monitrix.heritrix.LogFileEntry;
import uk.bl.monitrix.heritrix.LogFileEntry.DefaultAlert;
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
	
	// DB connection:
	private CassandraDBConnector db;
	
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

	public CassandraDBIngestConnector(DBConnector db) throws IOException {
		this.db = (CassandraDBConnector) db;
		this.init();
	}
		
	private void init() throws IOException {
		this.ingestSchedule = new CassandraIngestSchedule(db.getSession());
		this.crawlLogImporter = new CassandraCrawlLogImporter(db.getSession());
		this.alertLogImporter = new CassandraAlertLogImporter(db.getSession());
		this.knownHostImporter = new CassandraKnownHostImporter(db.getSession(), this.alertLogImporter);
		this.crawlStatsImporter = new CassandraCrawlStatsImporter(db.getSession(), knownHostImporter, new CassandraVirusLogImporter(db.getSession()));
		
		// Insert one automatically, if empty:
		/*
		if( this.ingestSchedule.getLogForCrawlerId("sample-crawler-id") == null ) {
			this.ingestSchedule.addLog(
					"/Users/andy/Documents/workspace/bl-crawler-tests/heritrix-3.1.2-SNAPSHOT/jobs/bl-test-crawl/heritrix/output/logs/bl-test-crawl/crawl.log.cp00001-20130605082749",
					//"/Users/andy/Documents/workspace/monitrix/test/sample-log-1E3.txt",
					"sample-crawler-id", 
					true
					);
		}
		*/
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
			
			List<DefaultAlert> alertBatch = new ArrayList<DefaultAlert>();
			
			int counter = 0; // Should be slightly faster than using list.size() to count
			int revisits = 0;
			long timeOfFirstLogEntryInBatch = Long.MAX_VALUE;
			long timeOfLastLogEntryInPatch = 0;
			while (iterator.hasNext() && (counter < CassandraProperties.BULK_INSERT_CHUNK_SIZE)) {
				LogFileEntry next = iterator.next();
				counter++;
				if( next.isRevisitRecord() ) revisits++;
				
				// Skip bad ones:
				if( next.getParseFailed() ) {
					Logger.error("Skipping storing a line due to a parse failure. "+counter);
					continue;
				}
				
				long timestamp = next.getLogTimestamp().getTime();
				if (timestamp < timeOfFirstLogEntryInBatch)
					timeOfFirstLogEntryInBatch = timestamp;
				if( timestamp > timeOfLastLogEntryInPatch)
					timeOfLastLogEntryInPatch = timestamp;
								
				// Store the log entry:
				crawlLogImporter.insert(next);

				// Update pre-aggregated stats
				crawlStatsImporter.update(next, crawlerId);
				
				// Host info
				knownHostImporter.addCrawlerID(next.getHost(), crawlerId);
				
				// FIXME Check for long runs and raise alerts?
				
				// Update stats and check for any host-level alerts:
				knownHostImporter.updateHostStats(next);
				
				// Log-entry-level alerts
				for (Alert a : next.getAlerts()) {
					alertBatch.add((DefaultAlert) a);
				}
				
				// Periodically update stats and flush the counter.
				if( counter == 200 ) {
					// Update last-seen date
					crawlLogImporter.updateCrawlInfo(crawlerId, timeOfFirstLogEntryInBatch, timeOfLastLogEntryInPatch );
				
					// Update the total log lines counter:
					ingestSchedule.incrementIngestedLogLines(logId, counter, revisits);
					counter = 0;
					revisits = 0;
				}
				
			}
			// Update the total log lines counter:
			ingestSchedule.incrementIngestedLogLines(logId, counter, revisits);
			
			// Update with final last-seen date
			crawlLogImporter.updateCrawlInfo(crawlerId, timeOfFirstLogEntryInBatch, timeOfLastLogEntryInPatch );
			
			Logger.info("Processed " + counter + " log entries (" + (System.currentTimeMillis() - bulkStart) + " ms) - writing to DB");
			bulkStart = System.currentTimeMillis();
			
			alertLogImporter.insert(alertBatch);
			alertBatch.clear();		
			
			Logger.info("Done (" + (System.currentTimeMillis() - bulkStart) + " ms)");			
		}
				
		Logger.debug("Done - took " + (System.currentTimeMillis() - start) + " ms");		
	}
	
}
