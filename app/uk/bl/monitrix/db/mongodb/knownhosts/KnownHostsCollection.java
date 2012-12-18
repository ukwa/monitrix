package uk.bl.monitrix.db.mongodb.knownhosts;

import java.util.HashSet;
import java.util.Set;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class KnownHostsCollection {
	
	private DBCollection collection;
	
	private Set<String> knownHostsCache = null;
	
	public KnownHostsCollection(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_KNOWN_HOSTS);
		
		// Known hosts collection is indexed by hostname (will be skipped automatically if index exists)
		this.collection.createIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, 1));
	}
	
	public boolean exists(String hostname) {
		if (knownHostsCache == null) {
			Set<String> knownHosts = new HashSet<String>();
			
			DBCursor cursor = collection.find();
			while (cursor.hasNext())
				knownHosts.add(new KnownHostsDBO(cursor.next()).getHostname());
			
			knownHostsCache = knownHosts;			
		}
		
		return knownHostsCache.contains(hostname);
	}
	
	public void addToList(String hostname, long lastAccess) {
		knownHostsCache.add(hostname);
		
		// TODO caching + bulk insert	
		KnownHostsDBO knownHost = new KnownHostsDBO(new BasicDBObject());
		knownHost.setHostname(hostname);
		knownHost.setLastAccess(lastAccess);
		collection.insert(knownHost.dbo);
	}
	
	public static String getHostFromURL(String url) {
		String host;
		if (url.startsWith("http://")) {
			host = url.substring(7);
		} else if (url.startsWith("https://")) {
			host = url.substring(8);
		} else if (url.startsWith("dns:")){
			host = url.substring(4);
		} else {
			// Should never happen
			throw new RuntimeException("Invalid URL: " + url);
		}
		
		host = host.substring(host.indexOf('.') + 1);
		
		if (host.indexOf("/") < 0)
			return host;
		
		return host.substring(0, host.indexOf("/"));
	}

}
