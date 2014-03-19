package uk.bl.monitrix.database.cassandra.model;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.KnownHost;

/**
 * A CassandraDB-backed implementation of {@link KnownHost}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 *
 */
public class CassandraKnownHost extends KnownHost {
	
	private static final String TABLE = CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_KNOWN_HOSTS;
	
	private Map<String, Object> cachedRow = new HashMap<String, Object>();
		
	public CassandraKnownHost(Row row) {
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME, row.getString(CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_TLD, row.getString(CassandraProperties.FIELD_KNOWN_HOSTS_TLD));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_DOMAIN, row.getString(CassandraProperties.FIELD_KNOWN_HOSTS_DOMAIN));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_SUBDOMAIN, row.getString(CassandraProperties.FIELD_KNOWN_HOSTS_SUBDOMAIN));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_FIRST_ACCESS, row.getLong(CassandraProperties.FIELD_KNOWN_HOSTS_FIRST_ACCESS));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS, row.getLong(CassandraProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLED_URLS, row.getLong(CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLED_URLS));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS, row.getLong(CassandraProperties.FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION, row.getDouble(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_RETRY_RATE, row.getDouble(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_RETRY_RATE));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE, row.getDouble(CassandraProperties.FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE, row.getDouble(CassandraProperties.FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_TEXT_TO_NONTEXT_RATIO, row.getDouble(CassandraProperties.FIELD_KNOWN_HOSTS_TEXT_TO_NONTEXT_RATIO));
		
		// Crawler IDs
		String crawlerIds = row.getString(CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLERS);
		Set<String> crawlerIdSet = new HashSet<String>(Arrays.asList(crawlerIds.split(";")));
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLERS, crawlerIdSet);
		
		// Map-type data fields
		deserializeMap(row, CassandraProperties.FIELD_KNOWN_HOSTS_FETCH_STATUS_CODES);
		deserializeMap(row, CassandraProperties.FIELD_KNOWN_HOSTS_CONTENT_TYPES);
		deserializeMap(row, CassandraProperties.FIELD_KNOWN_HOSTS_VIRUS_STATS);
	}
	
	private void deserializeMap(Row row, String key) {
		try {
			// We're using JSON to (de)serialize map-like data
			String serialized = row.getString(key);	
			if (serialized == null || serialized.isEmpty()) {
				cachedRow.put(key, new HashMap<String, Integer>());
			} else {
				@SuppressWarnings("unchecked")
				Map<String, Integer> deserialized = new ObjectMapper().readValue(serialized, HashMap.class);
				cachedRow.put(key, deserialized);
			}
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String getHostname() {
		return (String) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME);
	}
	
	@Override
	public String getTopLevelDomain() {
		return (String) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_TLD);
	}
	
	@Override
	public String getDomain() {
		return (String) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_DOMAIN);
	}
	@Override
	public String getSubdomain() {
		return (String) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_SUBDOMAIN);
	}
	
	@Override
	public long getFirstAccess() {
		return (Long) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_FIRST_ACCESS);
	}
	
	@Override
	public long getLastAccess() {
		return (Long) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS);
	}
	
	public void setLastAccess(long lastAccess) {
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS, lastAccess);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public List<String> getCrawlerIDs() {
		List<String> ids = new ArrayList<String>();
		ids.addAll((Set<String>) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLERS));
		return ids;
	}
	
	public void addCrawlerID(String id) {
		@SuppressWarnings("unchecked")
		Set<String> ids = (Set<String>) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLERS);
		ids.add(id);
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLERS, ids);
	}
	
	@Override
	public long getCrawledURLs() {
		Long crawledURLs = (Long) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLED_URLS);
		if (crawledURLs == null)
			return 0;
		
		return crawledURLs;
	}
	
	public void setCrawledURLs(long crawledURLs) {
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLED_URLS, crawledURLs);
	}
		
	@Override
	public long getSuccessfullyFetchedURLs() {
		return (Long) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS);
	}
	
	public void setSuccessfullyFetchedURLs(long urls) {
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS, urls);
	}
	
	@Override
	public double getAverageFetchDuration() {
		return (Double) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION);
	}
	
	public void setAverageFetchDuration(double duration) {
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION, duration);
	}

	@Override
	public double getAverageRetryRate() {
		return (Double) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_RETRY_RATE);
	}
	
	public void setAverageRetryRate(double rate) {
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_RETRY_RATE, rate);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Integer> getFetchStatusDistribution() {
		return (Map<String, Integer>) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_FETCH_STATUS_CODES);
	}
	
	public void setFetchStatusDistribution(Map<String, Integer> fetchStatusDistribution) {
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_FETCH_STATUS_CODES, fetchStatusDistribution);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Integer> getContentTypeDistribution() {
		return (Map<String, Integer>) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_CONTENT_TYPES);
	}
	
	public void setContentTypeDistribution(Map<String, Integer> contentTypeDistribution) {
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_CONTENT_TYPES, contentTypeDistribution);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Integer> getVirusStats() {
		return (Map<String, Integer>) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_VIRUS_STATS);
	}
	
	public void setVirusStats(Map<String, Integer> virusStats) {
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_VIRUS_STATS, virusStats);
	}
	
	@Override
	public double getRobotsBlockPercentage() {
		return (Double) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE);
	}
	
	public void setRobotsBlockPercentage(double percentage) {
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE, percentage);
	}
	
	@Override
	public double getRedirectPercentage() {
		return (Double) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE);
	}
	
	public void setRedirectPercentage(double percentage) {
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE, percentage);
	}
	
	@Override
	public double getTextToNoneTextRatio() {
		return (Double) cachedRow.get(CassandraProperties.FIELD_KNOWN_HOSTS_TEXT_TO_NONTEXT_RATIO);
	}
	
	public void setTextToNoneTextRatio(double ratio) {
		cachedRow.put(CassandraProperties.FIELD_KNOWN_HOSTS_TEXT_TO_NONTEXT_RATIO, ratio);
	}
	
	public void save(Session session) {
		String cql = "UPDATE " + TABLE + " SET ";
		for (Entry<String, Object> e : cachedRow.entrySet()) {
			if (!e.getKey().equals(CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME)) {
				if (e.getValue() instanceof String) {
					cql += e.getKey() + "='" + e.getValue() + "', ";
				} else if (e.getValue() instanceof Set) {
					@SuppressWarnings("unchecked")
					Set<String> set = (Set<String>) e.getValue();
					cql += e.getKey() + "='" + StringUtils.join(set, ";") + "', ";
				} else if (e.getValue() instanceof Map) {
					try {
						@SuppressWarnings("unchecked")
						Map<String, Integer> map = (Map<String, Integer>) e.getValue();
						
						StringWriter writer = new StringWriter();
						new ObjectMapper().writeValue(writer, map);
						String escaped = writer.toString().replace("'", "''");
						cql += e.getKey() + "='" + escaped + "', ";
					} catch (Exception exception) {
						exception.printStackTrace();
					}
				} else {
					cql += e.getKey() + "=" + e.getValue() + ", ";
				}
			}
		}
				
		// Eliminate last comma
		cql = cql.substring(0, cql.length() - 2);
		// Logger.info(cql);
		cql +=	" WHERE " + CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME + "='" + getHostname() + "';";
		session.execute(cql);
	}
	
}
