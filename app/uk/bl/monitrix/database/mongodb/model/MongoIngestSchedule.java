package uk.bl.monitrix.database.mongodb.model;

import java.util.ArrayList;
import java.util.List;

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
public class MongoIngestSchedule implements IngestSchedule {
	
	protected DBCollection collection; 
	
	public MongoIngestSchedule(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_INGEST_SCHEDULE);
	}

	@Override
	public IngestedLog addLog(String path, String crawlerId, boolean monitor) {
		// TODO check if this log is already in the DB (path and crawlerID must be unique) 
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
	public IngestedLog getLogForPath(String path) {
		DBObject dbo = collection.findOne(new BasicDBObject(MongoProperties.FIELD_INGEST_SCHEDULE_PATH, path));
		if (dbo == null)
			return null;
		
		return new MongoIngestedLog(dbo);
	}

	@Override
	public void setMonitoringEnabled(String id, boolean monitoringEnabled) {
		DBObject dbo = collection.findOne(new BasicDBObject(MongoProperties.FIELD_INGEST_SCHEDULE_ID, id));
		if (dbo != null) {
			new MongoIngestedLog(dbo).setIsMonitored(monitoringEnabled);
			collection.save(dbo);
		}
	}

}
