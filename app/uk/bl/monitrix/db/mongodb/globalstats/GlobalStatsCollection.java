package uk.bl.monitrix.db.mongodb.globalstats;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class GlobalStatsCollection {
	
	private DBCollection collection;
	
	public GlobalStatsCollection(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_GLOBAL_STATS);
	}
	
	public void save(GlobalStatsDBO dbo) {
		this.collection.save(dbo.dbo);
	}
	
	public GlobalStatsDBO getStats() {
		DBObject stats = collection.findOne();
		if (stats == null)
			return null;
		else
			return new GlobalStatsDBO(collection.findOne());
	}

}
