package uk.bl.monitrix.db.mongodb.heritrixlog;

import java.util.ArrayList;
import java.util.List;

import play.Logger;

import uk.bl.monitrix.CrawlLog;
import uk.bl.monitrix.db.mongodb.MongoProperties;
import uk.bl.monitrix.heritrix.LogEntry;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class HeritrixLogCollection implements CrawlLog {
		
	private DBCollection collection;
	
	// TODO make this dummy cache more flexible
	private List<LogEntry> hundredMostRecent = null;
	
	public HeritrixLogCollection(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_HERETRIX_LOG);
		
		// Heritrix Log collection is indexed by timestamp (will be skipped automatically if index exists)
		this.collection.createIndex(new BasicDBObject(MongoProperties.FIELD_LOG_TIMESTAMP, 1));
	}
	
	public void insert(List<HeritrixLogDBO> log) {
		collection.insert(HeritrixLogDBO.map(log));
	}
	
	@Override
	public List<LogEntry> getMostRecentEntries(int n) {
		if (hundredMostRecent == null) {
			Logger.info("Getting 100 most recent URLs from DB");
			DBCursor cursor = collection.find().sort(new BasicDBObject(MongoProperties.FIELD_LOG_TIMESTAMP, -1)).limit(100);
			
			List<LogEntry> recent = new ArrayList<LogEntry>();
			while(cursor.hasNext())
				recent.add(new LogEntry(new HeritrixLogDBO(cursor.next()).getLogLine()));
			
			hundredMostRecent = recent;
		}

		return hundredMostRecent;
	}

}
