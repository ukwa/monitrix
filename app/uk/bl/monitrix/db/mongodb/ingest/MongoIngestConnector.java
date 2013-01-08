package uk.bl.monitrix.db.mongodb.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import play.Logger;

import com.mongodb.BasicDBObject;

import uk.bl.monitrix.Alert;
import uk.bl.monitrix.CrawlLogEntry;
import uk.bl.monitrix.alerts.AlertPipeline;
import uk.bl.monitrix.db.IngestConnector;
import uk.bl.monitrix.db.mongodb.AbstractMongoConnector;
import uk.bl.monitrix.db.mongodb.MongoProperties;
import uk.bl.monitrix.db.mongodb.model.AlertsDBO;
import uk.bl.monitrix.db.mongodb.model.CrawlLogDBO;
import uk.bl.monitrix.db.mongodb.model.GlobalStatsDBO;

public class MongoIngestConnector extends AbstractMongoConnector implements IngestConnector {

	public MongoIngestConnector() throws IOException {
		super();
	}
	
	public MongoIngestConnector(String hostName, String dbName, int dbPort) throws IOException {
		super(hostName, dbName, dbPort);
	}

	@Override
	public void insert(Iterator<CrawlLogEntry> iterator) {
		Logger.info("Writing log to MongoDB");
		long start = System.currentTimeMillis();
		
		// Keep track of import for global stats
		long crawlStartTime = Long.MAX_VALUE;
		long crawlLastActivity = 0;
		long linesTotal = 0;
		
		AlertPipeline alertPipeline = new AlertPipeline();
		
		while (iterator.hasNext()) {
			long bulkStart = System.currentTimeMillis();
			
			List<CrawlLogDBO> bulk = new ArrayList<CrawlLogDBO>();
			
			int counter = 0; // Should be slightly faster than using list.size() to count
			while (iterator.hasNext() & counter < MongoProperties.BULK_INSERT_CHUNK_SIZE) {
				CrawlLogEntry next = iterator.next();
				counter++;
				
				// Update global stats
				long timestamp = next.getTimestamp().getTime();
				if (timestamp < crawlStartTime)
					crawlStartTime = timestamp;
				if (timestamp > crawlLastActivity)
					crawlLastActivity = timestamp;

				// Assemble the log DB entity
				CrawlLogDBO dbo = new CrawlLogDBO(new BasicDBObject());
				dbo.setTimestamp(timestamp);
				dbo.setHost(next.getHost());
				dbo.setCrawlerID(next.getCrawlerID());
				dbo.setHTTPCode(next.getHTTPCode());
				dbo.setLogLine(next.toString());
				bulk.add(dbo);	
				
				// Update pre-aggregated stats
				preAggregatedStatsCollection.update(next);
				
				// Check alerts
				for (Alert alert : alertPipeline.check(next)) {
					AlertsDBO alertDBO = new AlertsDBO(new BasicDBObject());
					alertDBO.setOffendingHost(next.getHost());
					alertDBO.setAlertName(alert.getAlertName());
					alertDBO.setAlertDescription(alert.getAlertDescription());
					alertsCollection.insert(alertDBO);
				}
			}
			
			linesTotal += counter;
			Logger.info("Processed " + counter + " log entries (" + (System.currentTimeMillis() - bulkStart) + " ms) - writing to DB");
			bulkStart = System.currentTimeMillis();
			heritrixLogCollection.insert(bulk);
			Logger.info("Done (" + (System.currentTimeMillis() - bulkStart) + " ms)");			
		}
		preAggregatedStatsCollection.commit();
		
		// Update global stats			
		GlobalStatsDBO stats = globalStatsCollection.getStats();
		if (stats == null) {
			stats = new GlobalStatsDBO(new BasicDBObject());
			stats.setCrawlStartTime(crawlStartTime);
			stats.setCrawlLastActivity(crawlLastActivity);
			stats.setLinesTotal(linesTotal);
		} else {	
			if (crawlStartTime < stats.getCrawlStartTime())
				stats.setCrawlStartTime(crawlStartTime);
			
			if (crawlLastActivity > stats.getCrawlLastActivity())
				stats.setCrawlLastActivity(crawlLastActivity);
			
			stats.setLinesTotal(stats.getLinesTotal() + linesTotal);
		}
		globalStatsCollection.save(stats);
				
		Logger.info("Done - took " + (System.currentTimeMillis() - start) + " ms");
	}

}
