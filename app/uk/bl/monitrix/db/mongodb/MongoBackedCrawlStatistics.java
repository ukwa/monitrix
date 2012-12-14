package uk.bl.monitrix.db.mongodb;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import uk.bl.monitrix.stats.CrawlStatistics;

/**
 * An implementation of {@link CrawlStatistics} backed by MongoDB. Currently (mostly) a dummy.
 * 
 * TODO finish, clean up, make more efficient
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoBackedCrawlStatistics implements CrawlStatistics {
	
	private long crawlStartTime;
	
	private long crawlLastActivity;
	
	MongoBackedCrawlStatistics(DB db) {
		DBCollection globalStats = db.getCollection(MongoConnector.COLLECTION_NAME_GLOBAL_STATS);
		DBObject stats = null;
		
		DBCursor cursor = globalStats.find();
		if (cursor.hasNext())
			stats = cursor.next();
		cursor.close();
		
		if (stats == null)
			throw new RuntimeException("Corrupt DB - Global crawl stats missing!");
		
		this.crawlStartTime = (Long) stats.get(MongoConnector.FIELD_CRAWL_START);
		this.crawlLastActivity = (Long) stats.get(MongoConnector.FIELD_CRAWL_LAST_ACTIVITIY); 
	}

	@Override
	public long getCrawlStartTime() {
		return crawlStartTime;
	}

	@Override
	public long getTimeOfLastCrawlActivity() {
		return this.crawlLastActivity;
	}

}
