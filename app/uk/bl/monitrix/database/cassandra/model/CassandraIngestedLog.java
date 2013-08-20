package uk.bl.monitrix.database.cassandra.model;

import org.bson.types.ObjectId;

import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.IngestedLog;

public class CassandraIngestedLog implements IngestedLog {
	
	private DBObject dbo;
	
	public CassandraIngestedLog(DBObject dbo) {
		this.dbo = dbo;
	}
	
	/**
	 * Returns the MongoDB entity that's backing this object.
	 * @return the DBObject
	 */
	public DBObject getBackingDBO() {
		return dbo;
	}

	@Override
	public String getId() {
		return ((ObjectId) dbo.get(MongoProperties.FIELD_INGEST_SCHEDULE_ID)).toString();
	}

	@Override
	public String getPath() {
		return (String) dbo.get(MongoProperties.FIELD_INGEST_SCHEDULE_PATH);
	}
	
	public void setPath(String path) {
		dbo.put(MongoProperties.FIELD_INGEST_SCHEDULE_PATH, path);
	}

	@Override
	public String getCrawlerId() {
		return (String) dbo.get(MongoProperties.FIELD_INGEST_SCHEDULE_CRAWLER_ID);
	}
	
	public void setCrawlerId(String id) {
		dbo.put(MongoProperties.FIELD_INGEST_SCHEDULE_CRAWLER_ID, id);
	}

	@Override
	public long getIngestedLines() {
		return (Long) dbo.get(MongoProperties.FIELD_INGEST_SCHEDULE_LINES);
	}
	
	public void setIngestedLines(long lines) {
		dbo.put(MongoProperties.FIELD_INGEST_SCHEDULE_LINES, lines);
	}

	@Override
	public boolean isMonitored() {
		return (Boolean) dbo.get(MongoProperties.FIELD_INGEST_SCHEDULE_MONITORED);
	}
	
	public void setIsMonitored(boolean isMonitored) {
		dbo.put(MongoProperties.FIELD_INGEST_SCHEDULE_MONITORED, isMonitored);
	}

}
