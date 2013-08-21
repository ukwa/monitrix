package uk.bl.monitrix.database.cassandra.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.KnownHost;
import uk.bl.monitrix.model.KnownHostList;
import uk.bl.monitrix.model.SearchResult;
import uk.bl.monitrix.model.SearchResultItem;

/**
 * A CassandraDB-backed implementation of {@link KnownHostList}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraKnownHostList implements KnownHostList {
	
	protected Session session;
	
	// A simple in-memory buffer for quick host lookups
	// private Set<String> knownHostsLookupCache = null;
	protected Map<String, CassandraKnownHost> cache = new HashMap<String, CassandraKnownHost>();
	
	public CassandraKnownHostList(Session session) {
		this.session = session;
	}
	
	@Override
	public long count() {
		ResultSet results = session.execute("SELECT COUNT(*) FROM crawl_uris.known_hosts;");
		return results.one().getLong("count");
	}
	
	@Override
	public long countSuccessful() {
		ResultSet results = session.execute("SELECT successfully_fetched_urls FROM crawl_uris.known_hosts;");
		long total = 0;
		Iterator<Row> rows = results.iterator();
		while( rows.hasNext() ) {
			total += rows.next().getLong("successfully_fetched_urls");
		}
		return total;
	}
	
	@Override
	public long getMaxFetchDuration() {
		Iterator<Row> rows = session.execute("SELECT * FROM crawl_uris.known_hosts;").iterator();
		long max = 0;
		while( rows.hasNext() ) {
			long fd = rows.next().getInt("fetch_duration");
			if (fd > max ) max = fd;
		}
		return max; 
	}

	@Override
	public boolean isKnown(String hostname) {
		if (cache.containsKey(hostname))
			return true;

		CassandraKnownHost wrapped = (CassandraKnownHost) getKnownHost(hostname);		
		if (wrapped == null)
			return false;

		cache.put(wrapped.getHostname(), wrapped);
		return true;
	}

	@Override
	public KnownHost getKnownHost(String hostname) {
		if (cache.containsKey(hostname))
			return cache.get(hostname);
		
		ResultSet results = session.execute("SELECT * FROM crawl_uris.known_hosts WHERE host='"+hostname+"';");
		if (results.isExhausted())
			return null;
		
		CassandraKnownHost wrapped = new CassandraKnownHost(results.one());
		cache.put(hostname, wrapped);
		return wrapped;
	}

	@Override
	public SearchResult searchHosts(String query, int limit, int offset) {		
//		// Parse query
//		List<String> tokens = Arrays.asList(KnownHost.tokenizeName(query));
//		DBObject q = new BasicDBObject(CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME_TOKENIZED, 
//				new BasicDBObject(MONGO_QUERY_ALL, tokens));
//		
//		return search(query, q, limit, offset);
		return null;
	}
	
	@Override
	public SearchResult searchByTopLevelDomain(String tld, int limit, int offset) {
//		DBObject q = new BasicDBObject(CassandraProperties.FIELD_KNOWN_HOSTS_TLD, tld);
//		return search(tld, q, limit, offset);
		return null;
	}
	
	@Override
	public SearchResult searchByAverageFetchDuration(long min, long max, int limit, int offset) {
//		DBObject query = new BasicDBObject(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION, 
//				new BasicDBObject("$gt", min).append("$lte", max));
//		return search(null, query, limit, offset);
		return null;
	}
	
	@Override
	public SearchResult searchByAverageRetries(int min, int max, int limit, int offset) {		
//		DBObject query = new BasicDBObject(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_RETRY_RATE, 
//				new BasicDBObject("$gte", min).append("$lt", max));
//		return search(null, query, limit, offset);
		return null;
	}
	
	@Override
	public SearchResult searchByRobotsBlockPercentage(double min, double max, int limit, int offset) {
//		DBObject query = new BasicDBObject(CassandraProperties.FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE,
//				new BasicDBObject("$gte", min).append("$lt", max));
//		return search(null, query, limit, offset);
		return null;
	}

	@Override
	public SearchResult searchByRedirectPercentage(double min, double max, int limit, int offset) {
//		DBObject query = new BasicDBObject(CassandraProperties.FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE,
//				new BasicDBObject("$gte", min).append("$lt", max));
//		return search(null, query, limit, offset);
		return null;
	}
	
//	private SearchResult search(String queryString, DBObject query, int limit, int offset) {
//		long startTime = System.currentTimeMillis();
//		long total = collection.count(query);
//					
//		List<SearchResultItem> hostnames = new ArrayList<SearchResultItem>();
//		
//		if (limit > 0) {
//			DBCursor cursor = collection.find(query).skip(offset).limit(limit);
//			
//			// Right now the number of URLs per host are packed into the 'description field' - not ideal!
//			// TODO we need to find a better way to handle 'search result metadata' 
//			while (cursor.hasNext()) {
//				KnownHost host = new CassandraKnownHost(cursor.next());
//				hostnames.add(new SearchResultItem(host.getHostname(), Long.toString(host.getCrawledURLs())));
//			}
//		}
//		
//		return new SearchResult(null, total, hostnames, limit, offset, System.currentTimeMillis() - startTime);
//		
//	}

	@Override
	public List<KnownHost> getCrawledHosts(long since) {
//		DBObject query = new BasicDBObject(CassandraProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS,
//				new BasicDBObject(MONGO_QUERY_GREATER_OR_EQUAL, since));
//		
//		List<KnownHost> hostnames = new ArrayList<KnownHost>();
//		DBCursor cursor = collection.find(query).sort(new BasicDBObject(CassandraProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS, -1));
//		while (cursor.hasNext())
//			hostnames.add(new CassandraKnownHost(cursor.next()));
//		
//		return hostnames;
		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<String> getTopLevelDomains() {
//		return (List<String>) collection.distinct(CassandraProperties.FIELD_KNOWN_HOSTS_TLD);
		return null;
	}
	
	@Override
	public long countForTopLevelDomain(String tld) {
//		return collection.count(new BasicDBObject(CassandraProperties.FIELD_KNOWN_HOSTS_TLD, tld));
		return 0;
	}
	
}
