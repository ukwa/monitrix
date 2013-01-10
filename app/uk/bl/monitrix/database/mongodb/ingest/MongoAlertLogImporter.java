package uk.bl.monitrix.database.mongodb.ingest;

import java.util.AbstractList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.database.mongodb.model.MongoAlert;
import uk.bl.monitrix.database.mongodb.model.MongoAlertLog;

class MongoAlertLogImporter extends MongoAlertLog {

	public MongoAlertLogImporter(DB db) {
		super(db);
		
		// Collection is indexed by timestamp and host (will be skipped automatically if index exists)
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_TIMESTAMP, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_OFFENDING_HOST, 1));
	}
	
	public void insert(final List<MongoAlert> alerts) {
		List<DBObject> mapped = new AbstractList<DBObject>() {
			@Override
			public DBObject get(int index) {
				return alerts.get(index).getBackingDBO();
			}

			@Override
			public int size() {
				return alerts.size();
			}
		};
		collection.insert(mapped);
	}

}
