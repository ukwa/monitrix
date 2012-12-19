package uk.bl.monitrix.db.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import play.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;

import uk.bl.monitrix.CrawlLog;
import uk.bl.monitrix.CrawlStatistics;
import uk.bl.monitrix.HostInformation;
import uk.bl.monitrix.db.DBConnector;
import uk.bl.monitrix.db.mongodb.globalstats.GlobalStatsCollection;
import uk.bl.monitrix.db.mongodb.globalstats.GlobalStatsDBO;
import uk.bl.monitrix.db.mongodb.heritrixlog.HeritrixLogCollection;
import uk.bl.monitrix.db.mongodb.heritrixlog.HeritrixLogDBO;
import uk.bl.monitrix.db.mongodb.knownhosts.KnownHostsCollection;
import uk.bl.monitrix.db.mongodb.knownhosts.KnownHostsDBO;
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
	
	/** Heretrix Log collection **/
	private HeritrixLogCollection heritrixLogCollection;
	
	/** Global Stats collection **/
	private GlobalStatsCollection globalStatsCollection;
	
	/** Known Hosts collection **/
	private KnownHostsCollection knownHosts;
	
	/** Pre-Aggregated Stats collection **/
	private PreAggregatedStatsCollection preAggregatedStatsCollection;
	
	public MongoConnector() throws IOException {
		init(MongoProperties.DB_HOST, MongoProperties.DB_NAME, MongoProperties.DB_PORT);
	}
	
	public MongoConnector(String hostName, String dbName, int dbPort) throws IOException {
		init(hostName, dbName, dbPort);
	}
	
	private void init(String hostName, String dbName, int dbPort) throws IOException {
		this.mongo = new Mongo(hostName, dbPort);
		this.db = mongo.getDB(dbName);
		this.heritrixLogCollection = new HeritrixLogCollection(db);
		this.globalStatsCollection = new GlobalStatsCollection(db);
		this.knownHosts = new KnownHostsCollection(db);
		this.preAggregatedStatsCollection = new PreAggregatedStatsCollection(db, knownHosts);
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
				dbo.setHost(next.getHost());
				dbo.setCrawlerID(next.getCrawlerID());
				dbo.setHTTPCode(next.getHTTPCode());
				dbo.setLogLine(next.toString());
				bulk.add(dbo);	
				
				// Update pre-aggregated stats
				preAggregatedStatsCollection.update(next);
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
	
	@Override
	public CrawlLog getCrawlLog() {
		return heritrixLogCollection;
	}
	
	@Override
	public CrawlStatistics getCrawlStatistics() {
		// Note: this means that every object calling this method gets their own copy, including their own cache!
		// TODO either use a central cache or only create a single MongoBackedCrawlStatistics instance
		return new MongoBackedCrawlStatistics(globalStatsCollection, preAggregatedStatsCollection);
	}
	
	@Override
	public HostInformation getHostInfo(String hostname) {
		KnownHostsDBO dbo = knownHosts.getHostInfo(hostname);
		if (dbo == null)
			return null;
		
		// Note: this means that every object calling this method gets their own copy, including their own cache!
		// TODO either use a central cache or only create a single MongoBackedHostInformation instance
		return new MongoBackedHostInformation(dbo, heritrixLogCollection);
	}
	
	@Override
	public void close() {
		this.mongo.close();
	}

}
