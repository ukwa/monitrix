package uk.bl.monitrix.database.cassandra.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import play.Logger;

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
	
	private static final String TABLE_HOSTS = CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_KNOWN_HOSTS;
	
	private static final String TABLE_TLDS = CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_KNOWN_TLDS;
	
	protected Session session;
	
	// A simple in-memory buffer for quick host lookups
	protected Map<String, CassandraKnownHost> cache = new HashMap<String, CassandraKnownHost>();
	
	public CassandraKnownHostList(Session session) {
		this.session = session;
	}
	
	@Override
	public long count() {
		ResultSet results = session.execute("SELECT COUNT(*) FROM " + TABLE_HOSTS + " ;");
		return results.one().getLong("count");
	}
	
	@Override
	public long countSuccessful() {
		ResultSet results = session.execute("SELECT " + CassandraProperties.FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS + " FROM " + TABLE_HOSTS + ";");
		long total = 0;
		Iterator<Row> rows = results.iterator();
		while (rows.hasNext()) {
			long fetched = rows.next().getLong(CassandraProperties.FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS);
			if (fetched > 0)
				total += 1;
		}
		
		return total;
	}
	
	@Override
	public long getMaxFetchDuration() {
		Iterator<Row> rows = session.execute("SELECT * FROM " + TABLE_HOSTS + ";").iterator();
		double max = 0;
		while( rows.hasNext() ) {
			double fd = rows.next().getDouble(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION);
			if (fd > max ) max = fd;
		}
		return Math.round(max);
	}

	@Override
	public boolean isKnown(String hostname) {
		if (cache.containsKey(hostname))
			return true;
		
		ResultSet results = session.execute("SELECT * FROM " + TABLE_HOSTS + " WHERE " + CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME + "='" + hostname + "';");
		if (results.isExhausted())
			return false;
		
		return true;
	}

	@Override
	public KnownHost getKnownHost(String hostname) {
		CassandraKnownHost knownHost = cache.get(hostname);
		if (knownHost == null) {
			ResultSet results = session.execute("SELECT * FROM " + TABLE_HOSTS + " WHERE " + CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME + "='" + hostname + "';");
			if (!results.isExhausted()) {
				knownHost = new CassandraKnownHost(results.one());
				cache.put(hostname, knownHost);
			}
		}
		
		return knownHost;
	}

	@Override
	public SearchResult searchHosts(String query, int limit, int offset) {		
		Iterator<Row> results = session.execute(
				"SELECT * FROM " + TABLE_HOSTS + " WHERE " + CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME + "='" + query + "' LIMIT " + (limit + offset) + ";")
				.iterator();
		
		for (int i=0; i<offset; i++) {
			if (results.hasNext())
				results.next();
		}
		
		return search(query, results, limit, offset);
	}
	
	@Override
	public SearchResult searchByTopLevelDomain(String tld, int limit, int offset) {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.known_hosts WHERE tld='"+tld+"';");
		return search(tld, results.iterator(), limit, offset);
	}
	
	   
	private SearchResult searchByRange(double min, double max, int limit, int offset, String property) {		
		List<Row> concatenated = new ArrayList<Row>();		
		for (String tld : getTopLevelDomains()) {
			String q = "SELECT * FROM " + TABLE_HOSTS + " WHERE " + property + " > " + min + " AND " + property + " < " + max +
					" AND " + CassandraProperties.FIELD_KNOWN_HOSTS_TLD + " ='" + tld + "'";
				
			if (limit > 0 && offset > 0) {
				q += " LIMIT " + (limit + offset) + " ALLOW FILTERING ;";
			} else {
				q+= " ALLOW FILTERING ;";
			}
			
			Iterator<Row> results = session.execute(q).iterator();
			for (int i=0; i<offset; i++) {
				if (results.hasNext())
					results.next();
			}
			
			while (results.hasNext())
				concatenated.add(results.next());
		}
		
		return search(null, concatenated.iterator(), limit, offset);
	}
	
	@Override
	public SearchResult searchByAverageFetchDuration(long min, long max, int limit, int offset) {
		return searchByRange(min, max, limit, offset, CassandraProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION);
	}
	
	@Override
	public SearchResult searchByAverageRetries(int min, int max, int limit, int offset) {
		return searchByRange(min, max, limit, offset, CassandraProperties.FIELD_KNOWN_HOSTS_AVG_RETRY_RATE);
	}
	
	@Override
	public SearchResult searchByRobotsBlockPercentage(double min, double max, int limit, int offset) {
		return searchByRange(min, max, limit, offset, CassandraProperties.FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE);
	}	
	

	@Override
	public SearchResult searchByRedirectPercentage(double min, double max, int limit, int offset) {
		return searchByRange(min, max, limit, offset, CassandraProperties.FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE);
	}
	
	private SearchResult search(String queryString, Iterator<Row> cursor, int limit, int offset) {
		long startTime = System.currentTimeMillis();
		long total = 0;
					
		List<SearchResultItem> hostnames = new ArrayList<SearchResultItem>();
		
		// Right now the number of URLs per host are packed into the 'description field' - not ideal!
		// TODO we need to find a better way to handle 'search result metadata'
		while (cursor.hasNext()) {
			KnownHost host = new CassandraKnownHost(cursor.next());
			hostnames.add(new SearchResultItem(host.getHostname(), Long.toString(host.getCrawledURLs())));
			// Update the total number of results.
			total++;
		}
		Logger.info("Total = "+total);
		
		return new SearchResult(null, total, hostnames, limit, offset, System.currentTimeMillis() - startTime);
		
	}

	/**
	 * FIXME this seems not to be called anywhere, so take it out as it's not simple to implement in Cassandra?
	 */
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
		return new ArrayList<KnownHost>();
	}

	@Override
	public List<String> getTopLevelDomains() {
		Iterator<Row> results =
				session.execute("SELECT " + CassandraProperties.FIELD_KNOWN_TLDS_TLD + " from " + TABLE_TLDS + ";").iterator();
		
		List<String> tlds = new ArrayList<String>();
		while (results.hasNext()) {
			String tld = results.next().getString(CassandraProperties.FIELD_KNOWN_TLDS_TLD);
			tlds.add(tld);
		}
		return tlds;
	}
	
	@Override
	public long countForTopLevelDomain(String tld) {
		ResultSet results = session.execute("SELECT * FROM " +  TABLE_TLDS + " WHERE " + CassandraProperties.FIELD_KNOWN_TLDS_TLD + "='" + tld + "';");
		return results.one().getLong(CassandraProperties.FIELD_KNOWN_TLDS_COUNT);
	}

	// FIXME Rounder
	protected double rounder_res = 100.0;
	protected double rounder_step = 1.0;
	protected double rounder(double in) {
		return Math.floor(rounder_res*in);
	}
	
}
