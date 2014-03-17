package uk.bl.monitrix.database.cassandra.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import play.Logger;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

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
		// TODO cache the rest
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
	public List<String> getCrawlerIDs() {
		return null;
	}
	
	@Override
	public long getCrawledURLs() {
		return -1;
	}
		
	@Override
	public long getSuccessfullyFetchedURLs() {
		return -1;
	}
	
	@Override
	public double getAverageFetchDuration() {
		return -1;
	}

	@Override
	public double getAverageRetryRate() {
		return -1;
	}
	
	@Override
	public Map<String, Integer> getFetchStatusDistribution() {
		return null;
	}

	@Override
	public Map<String, Integer> getContentTypeDistribution() {
		return null;
	}
	
	@Override
	public Map<String, Integer> getVirusStats() {
		return null;
	}
	
	@Override
	public double getRobotsBlockPercentage() {
		return -1;
	}
	
	@Override
	public double getRedirectPercentage() {
		return -1;
	}
	
	@Override
	public double getTextToNoneTextRatio() {
		return -1;
	}
	
	public void save(Session session) {
		String cql = "UPDATE " + TABLE + " SET ";
		for (Entry<String, Object> e : cachedRow.entrySet()) {
			if (!e.getKey().equals(CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME)) {
					cql += e.getKey() + "=";
				if (e.getValue() instanceof String)
					cql += "'" + e.getValue() + "', ";
				else
					cql += e.getValue() + ", ";
			}
		}
				
		// Eliminate last comma
		cql = cql.substring(0, cql.length() - 2);
		cql +=	" WHERE " + CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME + "='" + getHostname() + "';";
		Logger.info(cql);
		session.execute(cql);
	}
	
}
