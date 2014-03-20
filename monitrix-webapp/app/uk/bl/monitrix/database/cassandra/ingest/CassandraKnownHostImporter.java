package uk.bl.monitrix.database.cassandra.ingest;

import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import play.Logger;
import uk.bl.monitrix.analytics.HostAnalytics;
import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.database.cassandra.model.CassandraIngestSchedule;
import uk.bl.monitrix.database.cassandra.model.CassandraKnownHost;
import uk.bl.monitrix.database.cassandra.model.CassandraKnownHostList;

/**
 * An extended version of {@link CassandraKnownHostList} that adds insert/update capability.
 * 
 * TODO this whole class really needs some cleanup!
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class CassandraKnownHostImporter extends CassandraKnownHostList {
	
	// private static final String ALERT_MSG_TOO_MANY_SUBDOMAINS = "The host %s has a suspiciously high number of subdomains (%s)";
	
	// private static final String ALERT_MSG_TXT_TO_NONTEXT_RATIO = "The host %s serves a suspiciously high ratio of text vs. non-text resources";
	
	//  private CassandraAlertLogImporter alertLog;

	private PreparedStatement statementHosts;
	
	private PreparedStatement statementTLD;

	public CassandraKnownHostImporter(Session db, CassandraIngestSchedule ingestSchedule, CassandraAlertLogImporter alertLog) {
		super(db);
		
		// this.alertLog = alertLog;
		
		this.statementHosts = session.prepare(
				"INSERT INTO " + CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_KNOWN_HOSTS + " (" +
				CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_TLD + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_DOMAIN + ", " + 
				CassandraProperties.FIELD_KNOWN_HOSTS_SUBDOMAIN + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_FIRST_ACCESS + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLERS + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLED_URLS + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_AVG_RETRY_RATE + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_FETCH_STATUS_CODES + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_CONTENT_TYPES + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_VIRUS_STATS + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE + ", " +
				CassandraProperties.FIELD_KNOWN_HOSTS_TEXT_TO_NONTEXT_RATIO + ") " +
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");	
		
		this.statementTLD = session.prepare(
				"INSERT INTO " + CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_KNOWN_TLDS + " (" +
				CassandraProperties.FIELD_KNOWN_TLDS_TLD + ") " +
				"VALUES (?);");
	}
		
	/**
	 * Adds a new host to the Known Hosts list. Note that this method ONLY writes to
	 * the in-memory cache! In order to write to the database, execute the .commit() method
	 * after your additions are done.
	 * @param hostname the host name
	 * @param accessTime the access time
	 */
	public CassandraKnownHost addToList(String hostname, String domain, String subdomain, long accessTime) {	
		BoundStatement boundHostStatement = new BoundStatement(statementHosts);
		String tld = hostname.substring(hostname.lastIndexOf('.') + 1);
		session.execute(boundHostStatement.bind(
				hostname, tld, domain, subdomain,
				accessTime, accessTime,
				"", 0l, 0l, 0.0, 0.0, "", "", "", 0.0, 0.0, 0.0));
		
		BoundStatement boundTLDStatement = new BoundStatement(statementTLD);
		session.execute(boundTLDStatement.bind(tld));
		
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
		CassandraKnownHost host = (CassandraKnownHost) getKnownHost(hostname);
		if (host != null)
			host.setLastAccess(lastAccess);
		else
			Logger.warn("Attempt to write last access info to unknown host: " + hostname);
	}
	
	public void addCrawlerID(String hostname, String crawlerId) {
		// In this case we know it's a safe cast
		CassandraKnownHost host = (CassandraKnownHost) getKnownHost(hostname);
		if (host != null)
			host.addCrawlerID(crawlerId);
		else
			Logger.warn("Attempt to write crawlerID info to unknown host: " + hostname);
	}

	public void incrementFetchStatusCounter(String hostname, int fetchStatus) {
		// In this case we know it's a safe cast
		CassandraKnownHost host = (CassandraKnownHost) getKnownHost(hostname);
		if (host != null) {
			String key = Integer.toString(fetchStatus);
			Map<String, Integer> fetchStatusMap = host.getFetchStatusDistribution();
			Integer value = fetchStatusMap.get(key);
			if (value == null)
				fetchStatusMap.put(key, 1);
			else
				fetchStatusMap.put(key, value.intValue() + 1);
			host.setFetchStatusDistribution(fetchStatusMap);
		} else {
			Logger.warn("Attempt to write fetch status info to unknown host: " + hostname);
		}
	}
	
	public void incrementCrawledURLCounter(String hostname) {
		CassandraKnownHost host = (CassandraKnownHost) getKnownHost(hostname);
		if (host != null) {
			long crawledURLs = host.getCrawledURLs();
			host.setCrawledURLs(crawledURLs + 1);
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
				contentTypeMap.put(contentType, 1);
			else
				contentTypeMap.put(contentType, value.intValue() + 1);
			host.setContentTypeDistribution(contentTypeMap);
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
				virusMap.put(virusName, 1);
			else
				virusMap.put(virusName, value.intValue() + 1);
			host.setVirusStats(virusMap);
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
				
				host.setSuccessfullyFetchedURLs(successCount + 1);
				host.setAverageFetchDuration(newAvgResponseTime);
				host.setAverageRetryRate(newAvgRetryRate);	
			} else {
				Logger.warn("Attempt to update average response time for known host: " + hostname);
			}
		}
	}
	
	public void commit() {
		Logger.info("Updating known hosts list (" + cache.size() +  " hosts)");
		for (CassandraKnownHost knownHost : cache.values()) {
			knownHost.setRobotsBlockPercentage(HostAnalytics.computePercentageOfRobotsTxtBlocks(knownHost));
			knownHost.setRedirectPercentage(HostAnalytics.computePercentagOfRedirects(knownHost));
			knownHost.setTextToNoneTextRatio(HostAnalytics.computeTextToNonTextRatio(knownHost));			
			knownHost.save(session);
		}
	}

}
