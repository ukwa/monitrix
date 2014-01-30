package uk.bl.monitrix.database.cassandra.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
	
	private Row row;
	private Session session;
	
	public CassandraKnownHost(Session session, Row row) {
		this.row = row;
		this.session = session;
	}
	
	@Override
	public String getHostname() {
		return (String) row.getString(CassandraProperties.FIELD_KNOWN_HOSTS_HOSTNAME);
	}
	
	@Override
	public String getTopLevelDomain() {
		return (String) row.getString(CassandraProperties.FIELD_KNOWN_HOSTS_TLD);
	}
	
	@Override
	public String getDomain() {
		return (String) row.getString(CassandraProperties.FIELD_KNOWN_HOSTS_DOMAIN);
	}
	@Override
	public String getSubdomain() {
		return row.getString(CassandraProperties.FIELD_KNOWN_HOSTS_SUBDOMAIN);
	}
	
	@Override
	public long getFirstAccess() {
		return row.getDate(CassandraProperties.FIELD_KNOWN_HOSTS_FIRST_ACCESS).getTime();
	}
	
	@Override
	public long getLastAccess() {
		return row.getDate(CassandraProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS).getTime();
	}
	
	@Override
	public List<String> getCrawlerIDs() {
		return row.getList(CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLERS, String.class);
	}
	
	@Override
	public long getCrawledURLs() {
		return row.getLong(CassandraProperties.FIELD_KNOWN_HOSTS_CRAWLED_URLS);
	}
		
	@Override
	public long getSuccessfullyFetchedURLs() {
		return row.getLong(CassandraProperties.FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS);
	}
	
	@Override
	public double getAverageFetchDuration() {
		Double duration = row.getDouble(CassandraProperties.FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION);
		if (duration == null)
			return 0;
		else
			return duration;
	}

	@Override
	public double getAverageRetryRate() {
		Iterator<Row> rows = session.execute("SELECT * from crawl_uris.known_host_counters WHERE host='"+this.getHostname()+"';").iterator();
		if( rows.hasNext() ) {
			Row row = rows.next();
			return row.getLong("retries")/(double)row.getLong("successfully_fetched_uris");
		} else {
			return 0;
		}
	}
	
	@Override
	public Map<String, Integer> getFetchStatusDistribution() {
		Map<String, Integer> fetchStatusCodes = row.getMap(CassandraProperties.FIELD_KNOWN_HOSTS_FETCH_STATUS_CODES, String.class, Integer.class);
		if (fetchStatusCodes == null)
			return new HashMap<String, Integer>();
		return fetchStatusCodes;
	}

	@Override
	public Map<String, Integer> getContentTypeDistribution() {
		Map<String, Integer> contentTypeDistribution = row.getMap(CassandraProperties.FIELD_KNOWN_HOSTS_CONTENT_TYPES, String.class, Integer.class);
		if (contentTypeDistribution == null)
			return new HashMap<String, Integer>();
		
		return contentTypeDistribution;
	}
	
	@Override
	public Map<String, Integer> getVirusStats() {
		Map<String, Integer> virusStats = row.getMap(CassandraProperties.FIELD_KNOWN_HOSTS_VIRUS_STATS, String.class, Integer.class);
		if (virusStats == null)
			return new HashMap<String, Integer>();		
		return virusStats;
	}
	
	@Override
	public double getRobotsBlockPercentage() {
		return row.getDouble(CassandraProperties.FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE);
	}
	
	@Override
	public double getRedirectPercentage() {
		return row.getDouble(CassandraProperties.FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE);
	}
	
	@Override
	public double getTextToNoneTextRatio() {
		return row.getDouble(CassandraProperties.FIELD_KNOWN_HOSTS_TEXT_TO_NONTEXT_RATIO);
	}
	
}
