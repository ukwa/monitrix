package uk.bl.monitrix.database;

import uk.bl.monitrix.database.mongodb.MongoProperties;

import com.mongodb.DB;
import com.mongodb.DBCollection;

public class ExtensionTable {
	
	protected DBCollection collection;
	
	public ExtensionTable(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_ALERT_LOG);
	}

}
