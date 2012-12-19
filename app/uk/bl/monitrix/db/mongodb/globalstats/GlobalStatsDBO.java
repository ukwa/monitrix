package uk.bl.monitrix.db.mongodb.globalstats;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.DBObject;

/**
 * Wraps the DBObject stored in the MongoDB 'Global Stats' collection.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class GlobalStatsDBO {
	
	DBObject dbo;
	
	public GlobalStatsDBO(DBObject dbo) {
		this.dbo = dbo;
	}
	
	/**
	 * UNIX timestamp of the crawl start.
	 * @return the crawl start timestamp
	 */
	public long getCrawlStartTime() {
		return (Long) dbo.get(MongoProperties.FIELD_GLOBAL_CRAWL_START);
	}
	
	/**
	 * Sets the UNIX timestamp of the crawl start.
	 * @param crawlStartTime the crawl start timestamp
	 */
	public void setCrawlStartTime(long crawlStartTime) {
		dbo.put(MongoProperties.FIELD_GLOBAL_CRAWL_START, crawlStartTime);
	}
	
	/**
	 * UNIX timestamp of the last crawl activity, i.e. the timestamp of the most 
	 * recent log entry.
	 * @return timestamp of the last crawl activity
	 */
	public long getCrawlLastActivity() {
		return (Long) dbo.get(MongoProperties.FIELD_GLOBAL_CRAWL_LAST_ACTIVITIY);
	}
	
	/**
	 * Sets the UNIX timestamp of the last crawl activity.
	 * @param crawlLastActivity timestamp of the last crawl activity
	 */
	public void setCrawlLastActivity(long crawlLastActivity) {
		dbo.put(MongoProperties.FIELD_GLOBAL_CRAWL_LAST_ACTIVITIY, crawlLastActivity);
	}
	
	/**
	 * The total count of log entries in the DB.
	 * @return total count of log entries
	 */
	public long getLinesTotal() {
		return (Long) dbo.get(MongoProperties.FIELD_GLOBAL_LINES_TOTAL);
	}
	
	/**
	 * Sets the total count of log entries in the DB.
	 * @param linesTotal total count of log entries
	 */
	public void setLinesTotal(long linesTotal) {
		dbo.put(MongoProperties.FIELD_GLOBAL_LINES_TOTAL, linesTotal);
	}

}
