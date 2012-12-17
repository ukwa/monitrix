package uk.bl.monitrix.db.mongodb.globalstats;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.DBObject;

public class GlobalStatsDBO {
	
	DBObject dbo;
	
	public GlobalStatsDBO(DBObject dbo) {
		this.dbo = dbo;
	}
	
	public long getCrawlStartTime() {
		return (Long) dbo.get(MongoProperties.FIELD_GLOBAL_CRAWL_START);
	}
	
	public void setCrawlStartTime(long crawlStartTime) {
		dbo.put(MongoProperties.FIELD_GLOBAL_CRAWL_START, crawlStartTime);
	}
	
	public long getCrawlLastActivity() {
		return (Long) dbo.get(MongoProperties.FIELD_GLOBAL_CRAWL_LAST_ACTIVITIY);
	}
	
	public void setCrawlLastActivity(long crawlLastActivity) {
		dbo.put(MongoProperties.FIELD_GLOBAL_CRAWL_LAST_ACTIVITIY, crawlLastActivity);
	}
	
	public long getLinesTotal() {
		return (Long) dbo.get(MongoProperties.FIELD_GLOBAL_LINES_TOTAL);
	}
	
	public void setLinesTotal(long linesTotal) {
		dbo.put(MongoProperties.FIELD_GLOBAL_LINES_TOTAL, linesTotal);
	}

}
