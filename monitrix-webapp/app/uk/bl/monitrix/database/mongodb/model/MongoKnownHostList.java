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
import uk.bl.monitrix.model.SearchResult;
import uk.bl.monitrix.model.SearchResultItem;

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
		
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME_TOKENIZED, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_TLD, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS, -1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE, -1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE, -1));
	}
	
	@Override
	public long count() {
		return collection.count();
	}
	
	@Override
	public long countSuccessful() {
		DBObject query = new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS, new BasicDBObject("$exists", true));
		return collection.count(query);
	}
	
	@Override
	public long getMaxFetchDuration() {
		DBCursor cursor = collection.find().sort(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION, -1)).limit(1);
		if (cursor.hasNext()) {
			MongoKnownHost h = new MongoKnownHost(cursor.next());
			return (long) h.getAverageFetchDuration();
		}
		return 0; 
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
	public SearchResult searchHosts(String query, int limit, int offset) {		
		// Parse query
		List<String> tokens = Arrays.asList(KnownHost.tokenizeName(query));
		DBObject q = new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME_TOKENIZED, 
				new BasicDBObject(MONGO_QUERY_ALL, tokens));
		
		return search(query, q, limit, offset);
	}
	
	@Override
	public SearchResult searchByTopLevelDomain(String tld, int limit, int offset) {
		DBObject q = new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_TLD, tld);
		return search(tld, q, limit, offset);
	}
	
	@Override
	public SearchResult searchByAverageFetchDuration(long min, long max, int limit, int offset) {
		DBObject query = new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION, 
				new BasicDBObject("$gt", min).append("$lte", max));
		return search(null, query, limit, offset);
	}
	
	@Override
	public SearchResult searchByAverageRetries(int min, int max, int limit, int offset) {		
		DBObject query = new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_AVG_RETRY_RATE, 
				new BasicDBObject("$gte", min).append("$lt", max));
		return search(null, query, limit, offset);
	}
	
	@Override
	public SearchResult searchByRobotsBlockPercentage(double min, double max, int limit, int offset) {
		DBObject query = new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE,
				new BasicDBObject("$gte", min).append("$lt", max));
		return search(null, query, limit, offset);
	}

	@Override
	public SearchResult searchByRedirectPercentage(double min, double max, int limit, int offset) {
		DBObject query = new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE,
				new BasicDBObject("$gte", min).append("$lt", max));
		return search(null, query, limit, offset);
	}
	
	private SearchResult search(String queryString, DBObject query, int limit, int offset) {
		long startTime = System.currentTimeMillis();
		long total = collection.count(query);
					
		List<SearchResultItem> hostnames = new ArrayList<SearchResultItem>();
		
		if (limit > 0) {
			DBCursor cursor = collection.find(query).skip(offset).limit(limit);
			
			// Right now the number of URLs per host are packed into the 'description field' - not ideal!
			// TODO we need to find a better way to handle 'search result metadata' 
			while (cursor.hasNext()) {
				KnownHost host = new MongoKnownHost(cursor.next());
				hostnames.add(new SearchResultItem(host.getHostname(), Long.toString(host.getCrawledURLs())));
			}
		}
		
		return new SearchResult(null, total, hostnames, limit, offset, System.currentTimeMillis() - startTime);
		
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
	@SuppressWarnings("unchecked")
	public List<String> getTopLevelDomains() {
		return (List<String>) collection.distinct(MongoProperties.FIELD_KNOWN_HOSTS_TLD);
	}
	
	@Override
	public long countForTopLevelDomain(String tld) {
		return collection.count(new BasicDBObject(MongoProperties.FIELD_KNOWN_HOSTS_TLD, tld));
	}
	
}
