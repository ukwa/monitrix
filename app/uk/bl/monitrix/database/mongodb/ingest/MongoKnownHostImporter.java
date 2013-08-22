package uk.bl.monitrix.database.mongodb.ingest;

import java.util.ArrayList;
import java.util.Map;

import play.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;

import uk.bl.monitrix.analytics.HostAnalytics;
import uk.bl.monitrix.database.mongodb.model.MongoAlert;
import uk.bl.monitrix.database.mongodb.model.MongoKnownHost;
import uk.bl.monitrix.database.mongodb.model.MongoKnownHostList;
import uk.bl.monitrix.model.Alert.AlertType;

/**
 * An extended version of {@link MongoKnownHostList} that adds insert/update capability.
 * 
 * TODO this whole class really needs some cleanup!
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class MongoKnownHostImporter extends MongoKnownHostList {
	
	private static final String ALERT_MSG_TOO_MANY_SUBDOMAINS = "The host %s has a suspiciously high number of subdomains (%s)";
	
	private static final String ALERT_MSG_TXT_TO_NONTEXT_RATIO = "The host %s serves a suspiciously high ratio of text vs. non-text resources";
	
	private MongoAlertLogImporter alertLog;

	public MongoKnownHostImporter(DB db, MongoAlertLogImporter alertLog) {
		super(db);
		this.alertLog = alertLog;
	}
	
	/**
	 * Adds a new host to the Known Hosts list. Note that this method ONLY writes to
	 * the in-memory cache! In order to write to the database, execute the .commit() method
	 * after your additions are done.
	 * @param hostname the host name
	 * @param accessTime the access time
	 */
	public MongoKnownHost addToList(String hostname, String domain, String subdomain, long accessTime) {	
		MongoKnownHost knownHost = new MongoKnownHost(new BasicDBObject());
		knownHost.setHostname(hostname);
		knownHost.setTopLevelDomain(hostname.substring(hostname.lastIndexOf('.') + 1));
		knownHost.setDomain(domain);
		knownHost.setSubdomain(subdomain);
		knownHost.setFirstAccess(accessTime);
		knownHost.setLastAccess(accessTime);
		cache.put(hostname, knownHost);
		return knownHost;
	}
	
	/**
	 * Updates the last access time for the specified host. Note that this method ONLY 
	 * writes to the in-memory cache! In order to write to the database, execute the .commit()
	 * method after your additions are done.
	 * @param hostname the hostname
	 * @param lastAccess the new last access time
	 */
	public void setLastAccess(String hostname, long lastAccess) {		
		// In this case we know it's a safe cast
		MongoKnownHost dbo = (MongoKnownHost) getKnownHost(hostname);
		if (dbo != null)
			dbo.setLastAccess(lastAccess);
		else
			Logger.warn("Attempt to write last access info to unknown host: " + hostname);
	}
	
	public void addCrawlerID(String hostname, String crawlerId) {
		// In this case we know it's a safe cast
		MongoKnownHost dbo = (MongoKnownHost) getKnownHost(hostname);
		if (dbo != null)
			dbo.addCrawlerID(crawlerId);
		else
			Logger.warn("Attempt to write crawlerID info to unknown host: " + hostname);		
	}

	public void incrementFetchStatusCounter(String hostname, int fetchStatus) {
		// In this case we know it's a safe cast
		MongoKnownHost host = (MongoKnownHost) getKnownHost(hostname);
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
		MongoKnownHost host = (MongoKnownHost) getKnownHost(hostname);
		if (host != null) {
			long crawledURLs = host.getCrawledURLs();
			host.setCrawledURLs(crawledURLs + 1);
		} else {
			Logger.warn("Attempt to increment crawled URL counter for unknown host: " + hostname);
		}			
	}

	public void incrementContentTypeCounter(String hostname, String contentType) {
		// According to MongoDB rules: "fields stored in the db can't have . in them"
		contentType = contentType.replace('.', '@');		
		
		// In this case we know it's a safe cast
		MongoKnownHost host = (MongoKnownHost) getKnownHost(hostname);
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
		MongoKnownHost host = (MongoKnownHost) getKnownHost(hostname);
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
			MongoKnownHost host = (MongoKnownHost) getKnownHost(hostname);
			if (host != null) {
				long successCount = host.getSuccessfullyFetchedURLs();
				
				double currentAvgResponseTime = host.getAverageFetchDuration();
				double newAvgResponseTime = (currentAvgResponseTime * successCount + fetchDuration) / (successCount + 1);
				
				double currentAvgRetryRate = host.getAverageRetryRate();
				double newAvgRetryRate = (currentAvgRetryRate * successCount + retries) / (successCount + 1);
				
				host.setSuccessfullyFetchedURLs(successCount + 1);
				host.setAverageFetchDuration(newAvgResponseTime);
				host.setAverageRetryRate(newAvgRetryRate);
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
		for (MongoKnownHost knownHost : new ArrayList<MongoKnownHost>(cache.values())) {
			// Looks a little recursive... 
			knownHost.setRobotsBlockPercentage(HostAnalytics.computePercentageOfRobotsTxtBlocks(knownHost));
			knownHost.setRedirectPercentage(HostAnalytics.computePercentagOfRedirects(knownHost));
			knownHost.setTextToNoneTextRatio(HostAnalytics.computeTextToNonTextRatio(knownHost));
			
			collection.save(knownHost.getBackingDBO());
		}
		
		// Compute host-level alerts
		// Note: we only need to consider hosts that were added in this batch - i.e. those in the cache!
		// FIXME: Moving to the host-wise model means we've lost the alerts.
		Logger.info("Computing host-level alerts");
		for (MongoKnownHost host : cache.values()) {
			// Subdomain limit
			int subdomains = 1; //host.getSubdomain().size();
			if (subdomains > 100) {
				MongoAlert alert = new MongoAlert(new BasicDBObject());
				alert.setTimestamp(host.getLastAccess());
				alert.setOffendingHost(host.getHostname());
				alert.setAlertType(AlertType.TOO_MANY_SUBDOMAINS);
				alert.setAlertDescription(String.format(ALERT_MSG_TOO_MANY_SUBDOMAINS, host.getHostname(), Integer.toString(subdomains)));
				alertLog.insert(alert);
			}

			// Text-to-Nontext content type ratio limit
			if (host.getTextToNoneTextRatio() > 0.9) {
				MongoAlert alert = new MongoAlert(new BasicDBObject());
				alert.setTimestamp(host.getLastAccess());
				alert.setOffendingHost(host.getHostname());
				alert.setAlertType(AlertType.TXT_TO_NONTEXT_RATIO);
				alert.setAlertDescription(String.format(ALERT_MSG_TXT_TO_NONTEXT_RATIO, host.getHostname()));
				alertLog.insert(alert);
			}
		}
		
		cache.clear();
	}

}
