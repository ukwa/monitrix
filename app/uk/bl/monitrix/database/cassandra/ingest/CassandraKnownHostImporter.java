package uk.bl.monitrix.database.cassandra.ingest;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import play.Logger;
import uk.bl.monitrix.analytics.HostAnalytics;
import uk.bl.monitrix.database.cassandra.model.CassandraKnownHost;
import uk.bl.monitrix.database.cassandra.model.CassandraKnownHostList;
import uk.bl.monitrix.heritrix.LogFileEntry;
import uk.bl.monitrix.model.Alert.AlertType;
import uk.bl.monitrix.model.KnownHost;

/**
 * An extended version of {@link CassandraKnownHostList} that adds insert/update capability.
 * 
 * TODO this whole class really needs some cleanup!
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class CassandraKnownHostImporter extends CassandraKnownHostList {
	
	private static final String ALERT_MSG_TOO_MANY_SUBDOMAINS = "The host %s has a suspiciously high number of subdomains (%s)";
	
	private static final String ALERT_MSG_TXT_TO_NONTEXT_RATIO = "The host %s serves a suspiciously high ratio of text vs. non-text resources";
	
	private CassandraAlertLogImporter alertLog;

	private PreparedStatement statement;

	public CassandraKnownHostImporter(Session db, CassandraAlertLogImporter alertLog) {
		super(db);
		this.alertLog = alertLog;
		this.statement = session.prepare(
			      "INSERT INTO crawl_uris.known_hosts " +
			      "(host, tld, domain, subdomain, first_access, last_access, successfully_fetched_urls) " +
			      "VALUES (?, ?, ?, ?, ?, ?, ?);");
	}
	
	/**
	 * Adds a new host to the Known Hosts list. Note that this method ONLY writes to
	 * the in-memory cache! In order to write to the database, execute the .commit() method
	 * after your additions are done.
	 * @param hostname the host name
	 * @param accessTime the access time
	 */
	public CassandraKnownHost addToList(String hostname, String domain, String subdomain, long accessTime) {	
		BoundStatement boundStatement = new BoundStatement(statement);
		String tld = hostname.substring(hostname.lastIndexOf('.') + 1);
		session.execute(boundStatement.bind(
				hostname,
				tld,
				domain,
				subdomain,
				new Date(accessTime),
				new Date(accessTime),
				0L
				));
		session.execute("UPDATE crawl_uris.known_tlds SET crawled_urls = crawled_urls + 1 WHERE tld = '"+tld+"';");
		return (CassandraKnownHost) getKnownHost(hostname);
	}
	
	/**
	 * Updates the last access time for the specified host. Note that this method ONLY 
	 * writes to the in-memory cache! In order to write to the database, execute the .commit()
	 * method after your additions are done.
	 * @param hostname the hostname
	 * @param lastAccess the new last access time
	 */
	public void setLastAccess(String hostname, long lastAccess) {		
		session.execute("UPDATE crawl_uris.known_hosts SET last_access="+lastAccess+" WHERE host='"+hostname+"';");
	}
	
	public void addCrawlerID(String hostname, String crawlerId) {
		// In this case we know it's a safe cast
		CassandraKnownHost dbo = (CassandraKnownHost) getKnownHost(hostname);
		if (dbo != null) {
			List<String> cids = dbo.getCrawlerIDs();
			if( ! cids.contains(crawlerId)) {
				session.execute("UPDATE crawl_uris.known_hosts SET crawlers = crawlers + [ '"+crawlerId+"' ] WHERE host='"+hostname+"';");
			}
		}
		else {
			Logger.warn("Attempt to write crawlerID info to unknown host: " + hostname);
		}
	}

	public void incrementFetchStatusCounter(String hostname, int fetchStatus) {
		// In this case we know it's a safe cast
		CassandraKnownHost host = (CassandraKnownHost) getKnownHost(hostname);
		if (host != null) {
			String key = Integer.toString(fetchStatus);
			Map<String, Integer> fetchStatusMap = host.getFetchStatusDistribution();
			Integer value = fetchStatusMap.get(key);
			if (value == null)
				value = new Integer(1);
			else
				value = new Integer( value.intValue() + 1);
			session.execute("UPDATE crawl_uris.known_hosts SET fetch_status_codes = fetch_status_codes + { '"+key+"': "+value+" } WHERE host='"+hostname+"';");
		} else {
			Logger.warn("Attempt to write fetch status info to unknown host: " + hostname);
		}
	}
	
	public void incrementCrawledURLCounter(String hostname) {
		CassandraKnownHost host = (CassandraKnownHost) getKnownHost(hostname);
		if (host != null) {
			long crawledURLs = host.getCrawledURLs() + 1;
			session.execute("UPDATE crawl_uris.known_hosts SET crawled_urls="+crawledURLs+" WHERE host='"+hostname+"';");
		} else {
			Logger.warn("Attempt to increment crawled URL counter for unknown host: " + hostname);
		}			
	}

	public void incrementContentTypeCounter(String hostname, String contentType) {
		// In this case we know it's a safe cast
		CassandraKnownHost host = (CassandraKnownHost) getKnownHost(hostname);
		if (host != null) {
			Map<String, Integer> contentTypeMap = host.getContentTypeDistribution();
			Integer value = contentTypeMap.get(contentType);
			if (value == null)
				value = new Integer(1);
			else
				value = new Integer( value.intValue() + 1);
			session.execute("UPDATE crawl_uris.known_hosts SET content_types = content_types + { '"+contentType+"': "+value+" } WHERE host='"+hostname+"';");
		} else {
			Logger.warn("Attempt to write content type info to unknown host: " + hostname);
		}		
	}
	
	public void incrementVirusStats(String hostname, String virusName) {
		// In this case we know it's a safe cast
		CassandraKnownHost host = (CassandraKnownHost) getKnownHost(hostname);
		if (host != null) {
			Map<String, Integer> virusMap = host.getVirusStats();
			Integer value = virusMap.get(virusName);
			if (value == null)
				value = new Integer(1);
			else
				value = new Integer( value.intValue() + 1);
			session.execute("UPDATE crawl_uris.known_hosts SET virus_stats = virus_stats + { '"+virusName+"': "+value+" } WHERE host='"+hostname+"';");
		} else {
			Logger.warn("Attempt to write virus stats info to unknown host: " + hostname);
		}			
	}
	
	public void updateAverageResponseTimeAndRetryRate(String hostname, int fetchDuration, int retries) {
		if (fetchDuration > 0) {
			CassandraKnownHost host = (CassandraKnownHost) getKnownHost(hostname);
			if (host != null) {
				long successCount = host.getSuccessfullyFetchedURLs();
				
				double currentAvgResponseTime = host.getAverageFetchDuration();
				double newAvgResponseTime = (currentAvgResponseTime * successCount + fetchDuration) / (successCount + 1);
				
				double currentAvgRetryRate = host.getAverageRetryRate();
				double newAvgRetryRate = rounder((currentAvgRetryRate * successCount + retries) / (successCount + 1));
				
				session.execute("UPDATE crawl_uris.known_hosts SET successfully_fetched_urls="+(successCount + 1)+" WHERE host='"+hostname+"';");
				session.execute("UPDATE crawl_uris.known_hosts SET avg_fetch_duration="+newAvgResponseTime+" WHERE host='"+hostname+"';");
				session.execute("UPDATE crawl_uris.known_hosts SET avg_retry_rate="+newAvgRetryRate+" WHERE host='"+hostname+"';");
				// Update counter columns
				session.execute("UPDATE crawl_uris.known_host_counters SET successfully_fetched_uris = successfully_fetched_uris + 1, retries = retries + "+retries+", duration = duration + "+fetchDuration+" WHERE host='"+hostname+"';");
			} else {
				Logger.warn("Attempt to update average response time for known host: " + hostname);
			}
		}
	}
	
	/**
	 * Writes the contents of the cache to the database.
	 */
	public void updateHostStats( LogFileEntry l ) {
		String hostname = l.getHost();
		KnownHost host = this.getKnownHost(hostname);
		//Logger.info("Updating host stats");
		
		double d = HostAnalytics.computePercentageOfRobotsTxtBlocks(host);
		session.execute("UPDATE crawl_uris.known_hosts SET robots_block_percentage="+d+" WHERE host='"+hostname+"';");

		d = HostAnalytics.computePercentagOfRedirects(host);
		session.execute("UPDATE crawl_uris.known_hosts SET redirect_percentage="+d+" WHERE host='"+hostname+"';");
		
		d = HostAnalytics.computeTextToNonTextRatio(host);
		session.execute("UPDATE crawl_uris.known_hosts SET text_to_nontext_ratio="+d+" WHERE host='"+hostname+"';");
		
		// Compute host-level alerts
		// Note: we only need to consider hosts that were added in this batch - ie. those in the cache!
		//Logger.info("Computing host-level alerts");
		// Subdomain limit
		Iterator<Row> rows = session.execute("SELECT COUNT(*) FROM crawl_uris.known_hosts WHERE domain='"+l.getDomain()+"';").iterator();
		long subdomains = rows.next().getLong("count");
		// FIXME: Hard-coded alert level.
		if (subdomains > 100) {
			LogFileEntry.DefaultAlert alert = new LogFileEntry.DefaultAlert(
			host.getLastAccess(),
			host.getHostname(),
			AlertType.TOO_MANY_SUBDOMAINS,
			String.format(ALERT_MSG_TOO_MANY_SUBDOMAINS, host.getHostname(), Long.toString(subdomains)
			));
			alertLog.insert(alert);
		}

		// Text-to-Nontext content type ratio limit
		if (host.getTextToNoneTextRatio() > 0.9) {
			LogFileEntry.DefaultAlert alert = new LogFileEntry.DefaultAlert(
			host.getLastAccess(),
			host.getHostname(),
			AlertType.TXT_TO_NONTEXT_RATIO,
			String.format(ALERT_MSG_TXT_TO_NONTEXT_RATIO, host.getHostname()
			));
		alertLog.insert(alert);
		}
		
	}

}
