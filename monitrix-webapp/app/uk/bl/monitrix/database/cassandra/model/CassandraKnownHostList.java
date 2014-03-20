package uk.bl.monitrix.database.cassandra.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

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
		ResultSet results = session.execute("SELECT * FROM crawl_uris.known_hosts WHERE host='"+query+"';");
		return search(query, results.iterator(), limit, offset);
	}
	
	@Override
	public SearchResult searchByTopLevelDomain(String tld, int limit, int offset) {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.known_hosts WHERE tld='"+tld+"';");
		return search(tld, results.iterator(), limit, offset);
	}
	
	@Override
	public SearchResult searchByAverageFetchDuration(long min, long max, int limit, int offset) {
 		ResultSet results = session.execute(
 				"SELECT * FROM " + TABLE_HOSTS + " WHERE " + CassandraProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION + 
 				" > " + min + " AND " + CassandraProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION + " < " + max +
 				" AND " + CassandraProperties.FIELD_KNOWN_HOSTS_TLD + " IN ('" + StringUtils.join(getTopLevelDomains(), ",") + "');");
 		
		return search(null, results.iterator(), limit, offset);
	}
	
	@Override
	public SearchResult searchByAverageRetries(final int min, final int max, int limit, int offset) {
		// Attempting a clever inline iterator:
		Iterator<Row> multirows = new Iterator<Row>() {
			
			private Iterator<Row> cursor = null;
			double lo = rounder(min);
			double hi = rounder(max) - rounder_step; // Emulate less-than-or-equal-to.
			double cur = lo - rounder_step;
			
			@Override
			public boolean hasNext() {
				Logger.warn("hasNext...");
				while( cursor == null || (cur < hi && !cursor.hasNext())) {
					cur = cur + rounder_step;
					String cql = "SELECT * from crawl_uris.known_hosts where avg_retry_rate = "+cur+";";
					ResultSet results  = session.execute(cql);
					cursor = results.iterator();
					if( cursor.hasNext() ) {
				        Logger.info("CQL: "+cql);
					}
				}
				return cursor.hasNext();
			}

			@Override
			public Row next() {
				return cursor.next();
			}

			@Override
			public void remove() {
				// No-op
			}
		};
		return search(null, multirows, limit, offset);
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
		ResultSet results = session.execute("SELECT crawled_urls FROM crawl_uris.known_tlds WHERE tld='"+tld+"';");
		return results.one().getLong("crawled_urls");
	}

	// FIXME Rounder
	protected double rounder_res = 100.0;
	protected double rounder_step = 1.0;
	protected double rounder(double in) {
		return Math.floor(rounder_res*in);
	}
	
}
