package uk.bl.monitrix.db.mongodb.knownhosts;

import java.util.HashSet;
import java.util.Set;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Wraps the MongoDB 'Known Hosts' collection.
 * 
 * TODO needs real caching!
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class KnownHostsCollection {
	
	private DBCollection collection;
	
	// A simple in-memory buffer for quick host lookups
	private Set<String> knownHostsLookupCache = null;
	
	public KnownHostsCollection(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_KNOWN_HOSTS);
		
		// Known Hosts collection is indexed by hostname (will be skipped automatically if index exists)
		this.collection.createIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, 1));
	}
	
	private void initKnownLookupHostCache() {
		Set<String> knownHostLookupCache = new HashSet<String>();
		
		DBCursor cursor = collection.find();
		while (cursor.hasNext())
			knownHostLookupCache.add(new KnownHostsDBO(cursor.next()).getHostname());
		
		this.knownHostsLookupCache = knownHostLookupCache; 			
	}
	
	public boolean exists(String hostname) {
		if (knownHostsLookupCache == null)
			initKnownLookupHostCache();
		
		return knownHostsLookupCache.contains(hostname);
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
		
		knownHostsLookupCache.add(hostname);
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
