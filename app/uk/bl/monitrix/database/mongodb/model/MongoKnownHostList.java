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
 */
public class MongoKnownHostList implements KnownHostList {
	
	// MongoDB query operator for selecting documents where the field value equals any value in a list
	private static final String MONGO_QUERY_ALL = "$all";
	
	// MongoDB query operator for selecting documents where the field value is greater or equal to a specified value
	private static final String MONGO_QUERY_GREATER_OR_EQUAL = "$gte"; 
	
	protected DBCollection collection;
	
	// A simple in-memory buffer for quick host lookups
	// private Set<String> knownHostsLookupCache = null;
	protected Map<String, MongoKnownHost> cache = new HashMap<String, MongoKnownHost>();
	
	public MongoKnownHostList(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_KNOWN_HOSTS);
		
		// Known Hosts collection is indexed by hostname, tokenized host name, top-level domain and last-visit timestamp
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME_TOKENIZED, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_TLD, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS, 1));
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
	public HostSearchResult searchHosts(String query, int limit, int offset) {
		long startTime = System.currentTimeMillis();
		
		// Parse query
		List<String> tokens = Arrays.asList(KnownHost.tokenizeName(query));
		DBObject q = new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME_TOKENIZED, 
				new BasicDBObject(MONGO_QUERY_ALL, tokens));
		
		// Count total no. of hosts
		long total = collection.count(q);
		
		// Get result page
		List<String> hostnames = new ArrayList<String>();
		DBCursor cursor = collection.find(q).skip(offset).limit(limit);
		while (cursor.hasNext())
			hostnames.add(new MongoKnownHost(cursor.next()).getHostname());
		
		return new HostSearchResult(query, total, hostnames, limit, offset, System.currentTimeMillis() - startTime);
	}

	@Override
	public List<KnownHost> getCrawledHosts(long since) {
		DBObject query = new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS,
				new BasicDBObject(MONGO_QUERY_GREATER_OR_EQUAL, since));
		
		List<KnownHost> hostnames = new ArrayList<KnownHost>();
		DBCursor cursor = collection.find(query).sort(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS, -1));
		while (cursor.hasNext())
			hostnames.add(new MongoKnownHost(cursor.next()));
		
		return hostnames;
	}

	@Override
	public long countForTopLevelDomain(String tld) {
		return collection.count(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_TLD, tld));
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<String> getTopLevelDomains() {
		return (List<String>) collection.distinct(MongoProperties.FIELD_KNOWN_HOSTS_TLD);
	}

}
