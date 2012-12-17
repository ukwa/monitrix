package uk.bl.monitrix.db.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import play.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;

import uk.bl.monitrix.CrawlStatistics;
import uk.bl.monitrix.db.DBConnector;
import uk.bl.monitrix.db.mongodb.globalstats.GlobalStatsCollection;
import uk.bl.monitrix.db.mongodb.globalstats.GlobalStatsDBO;
import uk.bl.monitrix.db.mongodb.heritrixlog.HeritrixLogCollection;
import uk.bl.monitrix.db.mongodb.heritrixlog.HeritrixLogDBO;
import uk.bl.monitrix.db.mongodb.preaggregatedstats.PreAggregatedStatsCollection;
import uk.bl.monitrix.heritrix.LogEntry;

/**
 * An implementation of {@link DBConnector} for MongoDB.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoConnector implements DBConnector {	

	/** MongoDB host **/
	private Mongo mongo;
	
	/** Monitrix database **/
	private DB db;
	
	/** Global Stats collection **/
	private GlobalStatsCollection globalStatsCollection;
	
	/** Pre-Aggregated Stats collection **/
	private PreAggregatedStatsCollection preAggregatedStatsCollection;
	
	/** Heretrix Log collection **/
	private HeritrixLogCollection heritrixLogCollection;
	
	public MongoConnector() throws IOException {
		init(MongoProperties.DB_HOST, MongoProperties.DB_NAME, MongoProperties.DB_PORT);
	}
	
	public MongoConnector(String hostName, String dbName, int dbPort) throws IOException {
		init(hostName, dbName, dbPort);
	}
	
	private void init(String hostName, String dbName, int dbPort) throws IOException {
		this.mongo = new Mongo(hostName, dbPort);
		this.db = mongo.getDB(dbName);
		this.globalStatsCollection = new GlobalStatsCollection(db);
		this.preAggregatedStatsCollection = new PreAggregatedStatsCollection(db);
		this.heritrixLogCollection = new HeritrixLogCollection(db);
	}

	@Override
	public void insert(Iterator<LogEntry> iterator) {
		Logger.info("Writing log to MongoDB");
		long start = System.currentTimeMillis();
		
		// Keep track of import for global stats
		long crawlStartTime = Long.MAX_VALUE;
		long crawlLastActivity = 0;
		long linesTotal = 0;
		
		while (iterator.hasNext()) {
			long bulkStart = System.currentTimeMillis();
			
			List<HeritrixLogDBO> bulk = new ArrayList<HeritrixLogDBO>();
			
			int counter = 0; // Should be slightly faster than using list.size() to count
			while (iterator.hasNext() & counter < MongoProperties.BULK_INSERT_CHUNK_SIZE) {
				LogEntry next = iterator.next();
				counter++;
				
				// Update global stats
				long timestamp = next.getTimestamp().getTime();
				if (timestamp < crawlStartTime)
					crawlStartTime = timestamp;
				if (timestamp > crawlLastActivity)
					crawlLastActivity = timestamp;

				// Assemble the log DB entity
				HeritrixLogDBO dbo = new HeritrixLogDBO(new BasicDBObject());
				dbo.setTimestamp(timestamp);
				dbo.setLogLine(next.toString());
				bulk.add(dbo);	
				
				// Update pre-aggregated stats
				preAggregatedStatsCollection.update(next);
			}
			
			linesTotal += counter;
			heritrixLogCollection.insert(bulk);
			Logger.info("Wrote " + counter + " log entries to MongoDB - took " + (System.currentTimeMillis() - bulkStart) + " ms");			
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
	
	@Override
	public CrawlStatistics getCrawlStatistics() {
		return new MongoBackedCrawlStatistics(db);
	}
	
	@Override
	public void close() {
		this.mongo.close();
	}

}
