package uk.bl.monitrix.db.mongodb.heritrixlog;

import java.util.List;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class HeritrixLogCollection {
		
	private DBCollection collection;
	
	public HeritrixLogCollection(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_HERETRIX_LOG);
		
		// Heritrix Log collection is indexed by timestamp (will be skipped automatically if index exists)
		this.collection.createIndex(new BasicDBObject(MongoProperties.FIELD_LOG_TIMESTAMP, 1));
	}
	
	public void insert(List<HeritrixLogDBO> log) {
		collection.insert(HeritrixLogDBO.map(log));
	}

}
