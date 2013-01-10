package uk.bl.monitrix.database.mongodb.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.KnownHost;
import uk.bl.monitrix.model.KnownHostList;

/**
 * A MongoDB-backed implementation of {@link KnownHostList}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 *
 */
public class MongoKnownHostList implements KnownHostList {
	
	// MongoDB query operator for selecting documents where the field value equals any value in a list
	private static final String MONGO_QUERY_ALL = "$all";
	
	protected DBCollection collection;
	
	// A simple in-memory buffer for quick host lookups
	// private Set<String> knownHostsLookupCache = null;
	protected Map<String, MongoKnownHost> cache = new HashMap<String, MongoKnownHost>();
	
	public MongoKnownHostList(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_KNOWN_HOSTS);
		
		// Known Hosts collection is indexed by hostname and tokenized host name
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME_TOKENIZED, 1));
	}

	@Override
	public boolean isKnown(String hostname) {
		if (cache.containsKey(hostname))
			return true;
		
		DBObject dbo = collection.findOne(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, hostname));
		if (dbo == null)
			return false;

		MongoKnownHost wrapped = new MongoKnownHost(dbo);
		cache.put(wrapped.getHostname(), wrapped);
		return true;
	}

	@Override
	public KnownHost getKnownHost(String hostname) {
		if (cache.containsKey(hostname))
			return cache.get(hostname);
		
		DBObject dbo = collection.findOne(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, hostname));
		if (dbo == null)
			return null;
		
		MongoKnownHost wrapped = new MongoKnownHost(dbo);
		cache.put(hostname, wrapped);
		return wrapped;
	}

	@Override
	public List<String> searchHost(String query) {
		List<String> tokens = Arrays.asList(KnownHost.tokenizeName(query));
		DBObject q = new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME_TOKENIZED, 
				new BasicDBObject(MONGO_QUERY_ALL, tokens));
		
		List<String> hostnames = new ArrayList<String>();
		DBCursor cursor = collection.find(q);
		while (cursor.hasNext())
			hostnames.add(new MongoKnownHost(cursor.next()).getHostname());
		
		return hostnames;
	}

}
