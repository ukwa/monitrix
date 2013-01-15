package uk.bl.monitrix.database.mongodb.ingest;

import java.util.AbstractList;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.model.MongoAlert;
import uk.bl.monitrix.database.mongodb.model.MongoAlertLog;

class MongoAlertLogImporter extends MongoAlertLog {

	public MongoAlertLogImporter(DB db) {
		super(db);
	}
	
	public void insert(MongoAlert alert) {
		collection.insert(alert.getBackingDBO());
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
