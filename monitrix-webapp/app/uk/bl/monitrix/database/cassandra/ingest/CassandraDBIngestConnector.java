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
		this.alertLogImporter = new CassandraAlertLogImporter(db.getSession(), crawlLogImporter);
		this.knownHostImporter = new CassandraKnownHostImporter(db.getSession(), this.ingestSchedule, this.alertLogImporter);
		this.crawlStatsImporter = new CassandraCrawlStatsImporter(db.getSession(), ingestSchedule, knownHostImporter, new CassandraVirusLogImporter(db.getSession()));
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
			Logger.info("Processing next batch of log entries");
			
			while (iterator.hasNext() && (counter < CassandraProperties.BULK_INSERT_CHUNK_SIZE)) {		
				LogFileEntry next = iterator.next();
				counter++;
				
				if (next.isRevisitRecord())
					revisits++;
				
				// Skip bad ones:
				if (next.getParseFailed()) {
					Logger.error("Skipping storing a line due to a parse failure. " + counter);
					continue;
				}
				
				long timestamp = next.getLogTimestamp().getTime();
				if (timestamp < timeOfFirstLogEntryInBatch)
					timeOfFirstLogEntryInBatch = timestamp;
				if( timestamp > timeOfLastLogEntryInPatch)
					timeOfLastLogEntryInPatch = timestamp;
								
				crawlLogImporter.insert(next);
				crawlStatsImporter.update(next, crawlerId);
				knownHostImporter.addCrawlerID(next.getHost(), crawlerId);
				
				// FIXME Check for long runs and raise alerts?
				
				// Log-entry-level alerts
				for (Alert a : next.getAlerts()) {
					alertBatch.add((DefaultAlert) a);
				}
			}
			
			Logger.info("Updating crawl info");
			ingestSchedule.incrementIngestedLogLines(logId, counter, revisits);
			crawlLogImporter.updateCrawlInfo(crawlerId, timeOfFirstLogEntryInBatch, timeOfLastLogEntryInPatch );

			Logger.info("Inserting alerts");
			alertLogImporter.insert(alertBatch);
			alertBatch.clear();
			
			Logger.info("Committing crawl stats");
			crawlStatsImporter.commit();
			
			Logger.info("Batch ingest complete (" + (System.currentTimeMillis() - bulkStart) + " ms)");			
		}
				
		Logger.debug("Done - took " + (System.currentTimeMillis() - start) + " ms");		
	}
	
}
