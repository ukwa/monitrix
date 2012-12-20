package uk.bl.monitrix.db.mongodb.knownhosts;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import play.Logger;

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
	
	// MongoDB query operator for selecting documents where the field value equals any value in a list
	private static final String MONGO_ALL = "$all";
	
	private DBCollection collection;
	
	// A simple in-memory buffer for quick host lookups
	// private Set<String> knownHostsLookupCache = null;
	private Map<String, KnownHostsDBO> knownHostsLookupCache = null;
	
	public KnownHostsCollection(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_KNOWN_HOSTS);
		
		// Known Hosts collection is indexed by hostname and tokenized host name
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME_TOKENIZED, 1));
	}
	
	private void initKnownLookupHostCache() {
		// Set<String> knownHostsLookupCache = new HashSet<String>();
		Map<String, KnownHostsDBO> knownHostsLookupCache = new HashMap<String, KnownHostsDBO>();
		
		DBCursor cursor = collection.find();
		while (cursor.hasNext()) {
			KnownHostsDBO dbo = new KnownHostsDBO(cursor.next());
			knownHostsLookupCache.put(dbo.getHostname(), dbo);
		}
		
		this.knownHostsLookupCache = knownHostsLookupCache; 			
	}
	
	/**
	 * Checks if the host is already in the Known Hosts list. To minimize database
	 * access, this method will first check against an in-memory cache, and only
	 * against the database if the memory cache yielded no hit.
	 * @param hostname the host name
	 * @return <code>true</code> if the host is in the Known Hosts list
	 */
	public boolean exists(String hostname) {
		if (knownHostsLookupCache == null)
			initKnownLookupHostCache();
		
		if (knownHostsLookupCache.containsKey(hostname))
			return true;
		
		DBObject dbo = collection.findOne(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, hostname));
		if (dbo == null)
			return false;

		KnownHostsDBO wrapped = new KnownHostsDBO(dbo);
		knownHostsLookupCache.put(wrapped.getHostname(), wrapped);
		return true;
	}
	
	/**
	 * Returns the information for the specified host or <code>null</code> if the
	 * host was not found in the database.
	 * @param hostname the host name
	 * @return the host information
	 */
	public KnownHostsDBO getHostInfo(String hostname) {
		if (knownHostsLookupCache == null)
			initKnownLookupHostCache();
			
		if (knownHostsLookupCache.containsKey(hostname))
			return knownHostsLookupCache.get(hostname);
		
		DBObject dbo = collection.findOne(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, hostname));
		if (dbo == null)
			return null;
		
		KnownHostsDBO wrapped = new KnownHostsDBO(dbo);
		knownHostsLookupCache.put(hostname, wrapped);
		return wrapped;
	}
	
	public List<String> searchHost(String query) {
		List<String> tokens = Arrays.asList(tokenizeHostname(query));
		DBObject q = new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME_TOKENIZED, 
				new BasicDBObject(MONGO_ALL, tokens));
		
		List<String> hostnames = new ArrayList<String>();
		DBCursor cursor = collection.find(q);
		while (cursor.hasNext())
			hostnames.add(new KnownHostsDBO(cursor.next()).getHostname());
		
		return hostnames;
	}
	
	/**
	 * Adds a new host to the Known Hosts list.  Note that this method ONLY writes to
	 * the IN-MEMORY CACHE! In order to write to the database, execute the .commit() method
	 * after your additions are done.
	 * @param hostname the host name
	 * @param lastAccess the time of last access
	 */
	public void addToList(String hostname, long lastAccess) {	
		KnownHostsDBO dbo = new KnownHostsDBO(new BasicDBObject());
		dbo.setHostname(hostname);
		dbo.setLastAccess(lastAccess);
		knownHostsLookupCache.put(hostname, dbo);
	}
	
	public void commit() {
		// TODO risky - we'll lose updates to the database that haven't been made through this object!
		final List<KnownHostsDBO> cachedKnownHosts = new ArrayList<KnownHostsDBO>(knownHostsLookupCache.values());
		
		collection.drop();
		collection.insert(new AbstractList<DBObject>() {

			@Override
			public DBObject get(int index) {
				return cachedKnownHosts.get(index).dbo;
			}

			@Override
			public int size() {
				return cachedKnownHosts.size();
			}
			
		});
	}
	
	public void setLastAccess(String hostname, long lastAccess) {		
		KnownHostsDBO dbo = getHostInfo(hostname);
		if (dbo != null) {		
			dbo.setLastAccess(lastAccess);
			collection.save(dbo.dbo);
		} else {
			Logger.warn("Attempt to write last access info to unknown host: " + hostname);
		}
	}
	
	/**
	 * Helper method to split a host name into tokens. Host names
	 * will be split at the following characters: '.', '-', '_'
	 * 
	 * Note: keeping this in a separate method, although it's a
	 * one-liner. Possibly we want to do more elaborate things in the future.
	 * 
	 * @param hostname the host name
	 * @return the tokens
	 */
	public static String[] tokenizeHostname(String hostname) {
		return hostname.split("-|_|\\.");
	}
	
}
