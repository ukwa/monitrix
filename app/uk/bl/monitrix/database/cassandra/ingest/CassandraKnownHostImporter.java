package uk.bl.monitrix.database.cassandra.ingest;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import play.Logger;
import uk.bl.monitrix.analytics.HostAnalytics;
import uk.bl.monitrix.database.cassandra.model.CassandraAlert;
import uk.bl.monitrix.database.cassandra.model.CassandraKnownHost;
import uk.bl.monitrix.database.cassandra.model.CassandraKnownHostList;
import uk.bl.monitrix.model.Alert.AlertType;

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
			      "(host, tld, first_access, last_access, successfully_fetched_urls) " +
			      "VALUES (?, ?, ?, ?, ?);");
	}
	
	/**
	 * Adds a new host to the Known Hosts list. Note that this method ONLY writes to
	 * the in-memory cache! In order to write to the database, execute the .commit() method
	 * after your additions are done.
	 * @param hostname the host name
	 * @param accessTime the access time
	 */
	public CassandraKnownHost addToList(String hostname, long accessTime) {	
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(
				hostname,
				hostname.substring(hostname.lastIndexOf('.') + 1),
				new Date(accessTime),
				new Date(accessTime),
				0L
				));
		return getKnownHostFromDB(hostname);
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
	
	/**
	 * Adds a subdomain to the specified host. Note that this method ONLY
	 * writes to the in-memory cache! In order to write to the database, execute the .commit()
	 * method after your additions are done.
	 * @param hostname the hostname
	 * @param subdomain the subdomain to add
	 */
	public void addSubdomain(String hostname, String subdomain) {
		CassandraKnownHost item = getKnownHostFromDB(hostname);
		if (item != null) {
			List<String> subs = item.getSubdomains();
			if( ! subs.contains(subdomain)) {
				session.execute("UPDATE crawl_uris.known_hosts SET subdomains = subdomains + [ '"+subdomain+"' ] WHERE host='"+hostname+"';");
			}
		} else {
			Logger.warn("Attempt to write subdomain info to unknown host: " + hostname);
		}
	}
	
	public void addCrawlerID(String hostname, String crawlerId) {
		// In this case we know it's a safe cast
		CassandraKnownHost dbo = getKnownHostFromDB(hostname);
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
		CassandraKnownHost host = getKnownHostFromDB(hostname);
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
		CassandraKnownHost host = getKnownHostFromDB(hostname);
		if (host != null) {
			long crawledURLs = host.getCrawledURLs() + 1;
			Logger.info("Updating "+hostname+" "+crawledURLs);
			session.execute("UPDATE crawl_uris.known_hosts SET crawled_urls="+crawledURLs+" WHERE host='"+hostname+"';");
		} else {
			Logger.warn("Attempt to increment crawled URL counter for unknown host: " + hostname);
		}			
	}

	public void incrementContentTypeCounter(String hostname, String contentType) {
		// In this case we know it's a safe cast
		CassandraKnownHost host = getKnownHostFromDB(hostname);
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
		CassandraKnownHost host = getKnownHostFromDB(hostname);
		if (host != null) {
			Map<String, Integer> virusMap = host.getVirusStats();
			Integer value = virusMap.get(virusName);
			if (value == null)
				virusMap.put(virusName, 1);
			else
				virusMap.put(virusName, value.intValue() + 1);
//			host.setVirusStats(virusMap);
		} else {
			Logger.warn("Attempt to write virus stats info to unknown host: " + hostname);
		}			
	}
	
	public void updateAverageResponseTimeAndRetryRate(String hostname, int fetchDuration, int retries) {
		if (fetchDuration > 0) {
			CassandraKnownHost host = getKnownHostFromDB(hostname);
			if (host != null) {
				long successCount = host.getSuccessfullyFetchedURLs();
				
				double currentAvgResponseTime = host.getAverageFetchDuration();
				double newAvgResponseTime = (currentAvgResponseTime * successCount + fetchDuration) / (successCount + 1);
				
				double currentAvgRetryRate = host.getAverageRetryRate();
				double newAvgRetryRate = (currentAvgRetryRate * successCount + retries) / (successCount + 1);
				
				session.execute("UPDATE crawl_uris.known_hosts SET successfully_fetched_urls="+(successCount + 1)+" WHERE host='"+hostname+"';");
				session.execute("UPDATE crawl_uris.known_hosts SET avg_fetch_duration="+newAvgResponseTime+" WHERE host='"+hostname+"';");
				session.execute("UPDATE crawl_uris.known_hosts SET avg_retry_rate="+newAvgRetryRate+" WHERE host='"+hostname+"';");
			} else {
				Logger.warn("Attempt to update average response time for known host: " + hostname);
			}
		}
	}
	
	/**
	 * Writes the contents of the cache to the database.
	 */
	public void commit() {
		Logger.info("Updating known hosts list (" + cache.size() +  " hosts)");
		for (CassandraKnownHost knownHost : new ArrayList<CassandraKnownHost>(cache.values())) {
			// Looks a little recursive... 
//			knownHost.setRobotsBlockPercentage(HostAnalytics.computePercentageOfRobotsTxtBlocks(knownHost));
//			knownHost.setRedirectPercentage(HostAnalytics.computePercentagOfRedirects(knownHost));
//			knownHost.setTextToNoneTextRatio(HostAnalytics.computeTextToNonTextRatio(knownHost));
//			
//			collection.save(knownHost.getBackingDBO());
		}
		
		// Compute host-level alerts
		// Note: we only need to consider hosts that were added in this batch - ie. those in the cache!
		Logger.info("Computing host-level alerts");
		for (CassandraKnownHost host : cache.values()) {
			// Subdomain limit
			int subdomains = host.getSubdomains().size();
			if (subdomains > 100) {
//				CassandraAlert alert = new CassandraAlert(new BasicDBObject());
//				alert.setTimestamp(host.getLastAccess());
//				alert.setOffendingHost(host.getHostname());
//				alert.setAlertType(AlertType.TOO_MANY_SUBDOMAINS);
//				alert.setAlertDescription(String.format(ALERT_MSG_TOO_MANY_SUBDOMAINS, host.getHostname(), Integer.toString(subdomains)));
//				alertLog.insert(alert);
			}

			// Text-to-Nontext content type ratio limit
			if (host.getTextToNoneTextRatio() > 0.9) {
//				CassandraAlert alert = new CassandraAlert(new BasicDBObject());
//				alert.setTimestamp(host.getLastAccess());
//				alert.setOffendingHost(host.getHostname());
//				alert.setAlertType(AlertType.TXT_TO_NONTEXT_RATIO);
//				alert.setAlertDescription(String.format(ALERT_MSG_TXT_TO_NONTEXT_RATIO, host.getHostname()));
//				alertLog.insert(alert);
			}
		}
		
		cache.clear();
	}

}
