package uk.bl.monitrix.database.cassandra.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.KnownHost;

/**
 * A MongoDB-backed implementation of {@link KnownHost}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 *
 */
public class CassandraKnownHost extends KnownHost {
	
	private DBObject dbo;
	
	public CassandraKnownHost(DBObject dbo) {
		this.dbo = dbo;
	}
	
	/**
	 * Returns the MongoDB entity that's backing this object.
	 * @return the DBObject
	 */
	public DBObject getBackingDBO() {
		return dbo;
	}

	@Override
	public String getHostname() {
		return (String) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME);
	}
	
	public void setHostname(String hostname) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, hostname);
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME_TOKENIZED,
				Arrays.asList(KnownHost.tokenizeName(hostname)));
	}
	
	@Override
	public String getTopLevelDomain() {
		return (String) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_TLD);
	}
	
	public void setTopLevelDomain(String tld) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_TLD, tld);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public List<String> getSubdomains() {
		return (List<String>) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_SUBDOMAINS);
	}
	
	public void addSubdomain(String subdomain) {
		List<String> subdomains = getSubdomains();
		if (subdomains == null)
			subdomains = new ArrayList<String>();
		
		if (!subdomains.contains(subdomain))
			subdomains.add(subdomain);
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_SUBDOMAINS, subdomains);
		
		// TODO add tokenized subdomain name? (Could be useful for search)
	}

	@Override
	public long getFirstAccess() {
		return (Long) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_FIRST_ACCESS);
	}
	
	public void setFirstAccess(long firstAccess) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_FIRST_ACCESS, firstAccess);
	}

	@Override
	public long getLastAccess() {
		return (Long) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS);
	}
	
	public void setLastAccess(long lastAccess) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS, lastAccess);
	}
	

	@Override
	@SuppressWarnings("unchecked")
	public List<String> getCrawlerIDs() {
		List<String> crawlers = (List<String>) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_CRAWLERS);
		if (crawlers == null)
			return new ArrayList<String>();
		
		return crawlers;
	}
	
	public void addCrawlerID(String id) {
		List<String> crawlers = getCrawlerIDs();
		
		if (!crawlers.contains(id))
			crawlers.add(id);
		
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_CRAWLERS, crawlers);
	}

	@Override
	public long getCrawledURLs() {
		Long crawledURLs = (Long) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_CRAWLED_URLS);
		if (crawledURLs == null)
			return 0;
		else
			return crawledURLs;
	}
		
	public void setCrawledURLs(long urls) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_CRAWLED_URLS, urls);
	}
	
	@Override
	public long getSuccessfullyFetchedURLs() {
		Long fetchedURLs = (Long) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS);
		if (fetchedURLs == null)
			return 0;
		else
			return fetchedURLs;
	}
	
	public void setSuccessfullyFetchedURLs(long urls) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS, urls);
	}
		
	@Override
	public double getAverageFetchDuration() {
		Double duration = (Double) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION);
		if (duration == null)
			return 0;
		else
			return duration;
	}

	public void setAverageFetchDuration(double avg) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION, avg);			
	}
	
	@Override
	public double getAverageRetryRate() {
		Double retryRate = (Double) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_AVG_RETRY_RATE);
		if (retryRate == null)
			return 0;
		else
			return retryRate;
	}
	
	public void setAverageRetryRate(double rate) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_AVG_RETRY_RATE, rate);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Integer> getFetchStatusDistribution() {
		DBObject fetchStatusCodes = (DBObject) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_FETCH_STATUS_CODES);
		if (fetchStatusCodes == null)
			return new HashMap<String, Integer>();
		
		return fetchStatusCodes.toMap();
	}
	
	public void setFetchStatusDistribution(Map<String, Integer> fetchStatusDistribution) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_FETCH_STATUS_CODES, new BasicDBObject(fetchStatusDistribution));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Integer> getContentTypeDistribution() {
		DBObject contentTypeDistribution = (DBObject) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_CONTENT_TYPES);
		if (contentTypeDistribution == null)
			return new HashMap<String, Integer>();
		
		return contentTypeDistribution.toMap();
	}
	
	public void setContentTypeDistribution(Map<String, Integer> contentTypeDistribution) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_CONTENT_TYPES, new BasicDBObject(contentTypeDistribution));
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Integer> getVirusStats() {
		DBObject virusStats = (DBObject) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_VIRUS_STATS);
		if (virusStats == null)
			return new HashMap<String, Integer>();
		
		return virusStats.toMap();
	}
	
	public void setVirusStats(Map<String, Integer> virusStats) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_VIRUS_STATS, new BasicDBObject(virusStats));
	}
	
	@Override
	public double getRobotsBlockPercentage() {
		return (Double) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE);
	}
	
	public void setRobotsBlockPercentage(double percentage) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE, percentage);
	}

	@Override
	public double getRedirectPercentage() {
		return (Double) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE);
	}
	
	public void setRedirectPercentage(double percentage) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE, percentage);		
	}

	@Override
	public double getTextToNoneTextRatio() {
		return (Double) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_TEXT_TO_NONTEXT_RATIO);
	}
	
	public void setTextToNoneTextRatio(double ratio) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_TEXT_TO_NONTEXT_RATIO, ratio);
	}

}
