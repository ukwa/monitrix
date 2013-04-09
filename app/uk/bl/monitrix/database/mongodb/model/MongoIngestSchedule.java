package uk.bl.monitrix.database.mongodb.model;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.IngestSchedule;
import uk.bl.monitrix.model.IngestedLog;

/**
 * A MongoDB-backed implementation of {@link IngestSchedule}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoIngestSchedule extends IngestSchedule {
	
	protected DBCollection collection; 
	
	public MongoIngestSchedule(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_INGEST_SCHEDULE);
	}

	@Override
	public IngestedLog addLog(String path, String crawlerId, boolean monitor) {
		// Check if this log is already in the DB (path and crawlerID must be unique):
		if( getLogForCrawlerId(crawlerId) != null ) return null;
		if( getLogForPath(path) != null ) return null;
		// Add the log:
		MongoIngestedLog log = new MongoIngestedLog(new BasicDBObject());
		log.setPath(path);
		log.setCrawlerId(crawlerId);
		log.setIsMonitored(monitor);
		log.setIngestedLines(0);
		collection.insert(log.getBackingDBO());
		return log;
	}
	
	@Override
	public List<IngestedLog> getLogs() {
		List<IngestedLog> logs = new ArrayList<IngestedLog>();
		DBCursor cursor = collection.find();
		while(cursor.hasNext())
			logs.add(new MongoIngestedLog(cursor.next()));
		return logs;
	}
	
	@Override
	public IngestedLog getLog(String id) {
		DBObject dbo = collection.findOne(new BasicDBObject(MongoProperties.FIELD_INGEST_SCHEDULE_ID, new ObjectId(id)));
		if (dbo == null)
			return null;
		
		return new MongoIngestedLog(dbo);
	}
	
	@Override
	public IngestedLog getLogForCrawlerId(String crawlerId) {
		DBObject dbo = collection.findOne(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_CRAWLER_ID, crawlerId));
		if (dbo == null)
			return null;
		
		return new MongoIngestedLog(dbo);
	}
	
	@Override
	public IngestedLog getLogForPath(String path) {
		DBObject dbo = collection.findOne(new BasicDBObject(MongoProperties.FIELD_INGEST_SCHEDULE_PATH, path));
		if (dbo == null)
			return null;
		
		return new MongoIngestedLog(dbo);
	}
	
	@Override
	public boolean isMonitoringEnabled(String id) {
		IngestedLog log = getLog(id);
		if (log == null)
			return false;
		
		return log.isMonitored();
	}

	@Override
	public void setMonitoringEnabled(String id, boolean monitoringEnabled) {
		MongoIngestedLog log = (MongoIngestedLog) getLog(id);
		if (log != null) {
			log.setIsMonitored(monitoringEnabled);
			collection.save(log.getBackingDBO());
		}
	}
	
	public void incrementIngestedLogLines(String id, long increment) {
		MongoIngestedLog log = (MongoIngestedLog) getLog(id);
		if (log != null) {
			log.setIngestedLines(log.getIngestedLines() + increment);
			collection.save(log.getBackingDBO());
		}
	}

}
