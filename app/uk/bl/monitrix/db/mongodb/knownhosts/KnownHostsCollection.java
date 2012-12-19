package uk.bl.monitrix.db.mongodb.knownhosts;

import java.util.HashSet;
import java.util.Set;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class KnownHostsCollection {
	
	private DBCollection collection;
	
	private Set<String> knownHostsList = null;
	
	public KnownHostsCollection(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_KNOWN_HOSTS);
		
		// Known hosts collection is indexed by hostname (will be skipped automatically if index exists)
		this.collection.createIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, 1));
	}
	
	private void initKnownHostCache() {
		Set<String> knownHostsList = new HashSet<String>();
		
		DBCursor cursor = collection.find();
		while (cursor.hasNext()) {
			KnownHostsDBO dbo = new KnownHostsDBO(cursor.next()); 
			knownHostsList.add(dbo.getHostname());
		}
		
		this.knownHostsList = knownHostsList; 			
	}
	
	public boolean exists(String hostname) {
		if (knownHostsList == null)
			initKnownHostCache();
		
		return knownHostsList.contains(hostname);
	}
	
	public KnownHostsDBO getHostInfo(String hostname) {
		DBObject dbo = collection.findOne(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, hostname));
		if (dbo == null)
			return null;
		
		return new KnownHostsDBO(dbo);
	}
	
	public void addToList(String hostname, long lastAccess) {	
		// TODO caching + bulk insert	
		KnownHostsDBO knownHost = new KnownHostsDBO(new BasicDBObject());
		knownHost.setHostname(hostname);
		knownHost.setLastAccess(lastAccess);
		collection.insert(knownHost.dbo);
		
		knownHostsList.add(hostname);
	}
	
	public void setLastAccess(String hostname, long lastAccess) {		
		/*
		DBObject dbo = 
				collection.findOne(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, hostname));
		
		// TODO maybe we should handle this more gracefully?
		if (dbo == null)
			throw new RuntimeException(hostname + " not found in known hosts list");
		
		KnownHostsDBO updatedDBO = new KnownHostsDBO(dbo);
		updatedDBO.setLastAccess(lastAccess);
		collection.save(updatedDBO.dbo);
		
		knownHostsList.add(hostname);
		*/
	}
	
}
