package uk.bl.monitrix.db.mongodb.globalstats;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Wraps the MongoDB 'Global Stats' collection.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class GlobalStatsCollection {
	
	private DBCollection collection;
	
	public GlobalStatsCollection(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_GLOBAL_STATS);
	}
	
	/**
	 * Saves the wrapped DBObject to the collection.
	 * @param dbo the DBObject
	 */
	public void save(GlobalStatsDBO dbo) {
		this.collection.save(dbo.dbo);
	}
	
	/**
	 * Returns the global stats object from the collection, or <code>null</code>
	 * if the object has not been created yet.
	 * @return the global stats object, or <code>null</code>
	 */
	public GlobalStatsDBO getStats() {
		DBObject stats = collection.findOne();
		if (stats == null)
			return null;
		else
			return new GlobalStatsDBO(collection.findOne());
	}

}
