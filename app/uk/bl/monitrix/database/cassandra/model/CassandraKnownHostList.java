package uk.bl.monitrix.database.cassandra.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import play.Logger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlLogEntry;
import uk.bl.monitrix.model.CrawlLogEntry;
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
			long fetched = rows.next().getLong("successfully_fetched_urls");
			if( fetched > 0 )
				total += 1;
		}
		return total;
	}
	
	@Override
	public long getMaxFetchDuration() {
		Iterator<Row> rows = session.execute("SELECT * FROM crawl_uris.known_hosts;").iterator();
		double max = 0;
		while( rows.hasNext() ) {
			double fd = rows.next().getDouble("avg_fetch_duration");
			if (fd > max ) max = fd;
		}
		return Math.round(max);
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
		ResultSet results = session.execute("SELECT * FROM crawl_uris.known_hosts WHERE host='"+hostname+"';");
		if (results.isExhausted())
			return null;
		
		CassandraKnownHost wrapped = new CassandraKnownHost(session,results.one());
		return wrapped;
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
//		DBObject query = new BasicDBObject(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION, 
//				new BasicDBObject("$gt", min).append("$lte", max));
//		return search(null, query, limit, offset);
		return null;
	}
	
	@Override
	public SearchResult searchByAverageRetries(final int min, final int max, int limit, int offset) {
		// Attempting a clever inline iterator:
		Iterator<Row> multirows = new Iterator<Row>() {
			
			private Iterator<Row> cursor = null;
			double lo = rounder(min);
			double hi = rounder(max) - 1; // Emulate less-than-or-equal-to.
			double cur = lo - 1;
			
			@Override
			public boolean hasNext() {
				Logger.warn("hasNext...");
				while( cursor == null || (cur < hi && !cursor.hasNext())) {
					cur = cur + 1;
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
			KnownHost host = new CassandraKnownHost(session,cursor.next());
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
		return null;
	}

	@Override
	public List<String> getTopLevelDomains() {
		List<String> tlds = new ArrayList<String>();
		Iterator<Row> rows = session.execute("SELECT tld from crawl_uris.known_tlds;").iterator();
		// Loop over other tlds:
		while( rows.hasNext() ) {
			String tld = rows.next().getString("tld");
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
	protected double rounder_res = 1000.0;
	protected double rounder_step = 1.0 / rounder_res;
	protected double rounder(double in) {
		return Math.floor(rounder_res*in);
	}
	
}
