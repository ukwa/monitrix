package uk.bl.monitrix.database.cassandra.ingest;

import play.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.database.cassandra.model.CassandraCrawlStats;
import uk.bl.monitrix.database.cassandra.model.CassandraCrawlStatsUnit;
import uk.bl.monitrix.model.CrawlLogEntry;
import uk.bl.monitrix.model.IngestSchedule;
import uk.bl.monitrix.model.KnownHost;

/**
 * An extended version of {@link CassandraCrawlStats} that adds ingest capability.
 * The ingest is 'smart' in the sense as it also performs various aggregation computations,
 * including those involving the known hosts list.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class CassandraCrawlStatsImporter extends CassandraCrawlStats {
	
	private PreparedStatement statement = null;
	
	private CassandraKnownHostImporter knownHosts;
	// private CassandraVirusLogImporter virusLog;
	
	public CassandraCrawlStatsImporter(Session db, IngestSchedule schedule, CassandraKnownHostImporter knownHosts, CassandraVirusLogImporter virusLog) {
		super(db, schedule);
		
		this.knownHosts = knownHosts;
		// this.virusLog = virusLog;
		
		this.statement = session.prepare(
				"INSERT INTO " + CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_CRAWL_STATS + " (" +
				CassandraProperties.FIELD_CRAWL_STATS_CRAWL_ID + ", " +
				CassandraProperties.FIELD_CRAWL_STATS_TIMESTAMP + ", " +
				CassandraProperties.FIELD_CRAWL_STATS_DOWNLOAD_VOLUME + ", " + 
				CassandraProperties.FIELD_CRAWL_STATS_NUMBER_OF_URLS_CRAWLED + ", " +
				CassandraProperties.FIELD_CRAWL_STATS_NEW_HOSTS_CRAWLED + ", " + 
				CassandraProperties.FIELD_CRAWL_STATS_COMPLETED_HOSTS + ") " +
				"VALUES (?, ?, ?, ?, ?, ?);");		
	}
	
	/**
	 * Updates the crawl stats with a single log entry. Note that this method ONLY writes to
	 * the in-memory cache to avoid excessive DB transactions! To write to the DB, execute the
	 * .commit() method after your updates are done.
	 * @param entry the log entry
	 */
	public void update(CrawlLogEntry entry, String crawl_id) {
		// Step 1 - compute the timeslot
		long timeslot = toTimeslot(entry.getLogTimestamp().getTime());
				
		// Step 2 - update data for this timeslot
		CassandraCrawlStatsUnit currentUnit = (CassandraCrawlStatsUnit) getStatsForTimestamp(timeslot, crawl_id);
		if (currentUnit == null) {
			BoundStatement boundStatement = new BoundStatement(statement);
			session.execute(boundStatement.bind(
					crawl_id,
					timeslot,
					entry.getDownloadSize(),
					1l, 0l, 0l));
			
			// This also ensures we got the unit in the cache now
			currentUnit = (CassandraCrawlStatsUnit) getStatsForTimestamp(timeslot, crawl_id);
		} else {
			currentUnit.setDownloadVolume(currentUnit.getDownloadVolume() + entry.getDownloadSize());
			currentUnit.setNumberOfURLsCrawled(currentUnit.getNumberOfURLsCrawled() + 1);
		}
		
		// Step 3 - update hosts info
		String hostname = entry.getHost();
		if (knownHosts.isKnown(hostname)) {
			Logger.info("Updating existing host");
			KnownHost host = knownHosts.getKnownHost(hostname);
			
			// Update host completion time
			long lastRecordedAccess = host.getLastAccess();
			if (lastRecordedAccess < timeslot) {
				// MongoCrawlStatsUnit unitToModify = (MongoCrawlStatsUnit) getStatsForTimestamp(toTimeslot(lastRecordedAccess), crawl_id);
				// unitToModify.setCompletedHosts(unitToModify.countCompletedHosts() - 1);
				// currentUnit.setCompletedHosts(currentUnit.countCompletedHosts() + 1);
			}
			
			// Update last access time
			// knownHosts.setLastAccess(hostname, entry.getLogTimestamp().getTime());
		} else {
			Logger.info("Registering new host: " + hostname);
			long timestamp = entry.getLogTimestamp().getTime();
			knownHosts.addToList(hostname, entry.getDomain(), entry.getSubdomain(), timestamp);
			currentUnit.setNumberOfNewHostsCrawled(currentUnit.getNumberOfNewHostsCrawled() + 1);
			currentUnit.setCompletedHosts(currentUnit.countCompletedHosts() + 1);
		}
		
		// Note: it's a little confusing that these aggregation steps are in this class
		// TODO move into the main CassandraBatchImporter
		// knownHosts.incrementFetchStatusCounter(hostname, entry.getHTTPCode());
		// knownHosts.incrementCrawledURLCounter(hostname);
		// knownHosts.updateAverageResponseTimeAndRetryRate(hostname, entry.getFetchDuration(), entry.getRetries());
		
		// Warning: there seems to be a bug in Heritrix which sometimes leaves a 'content type template' (?)
		// in the log line: content type = '$ctype'. This causes CassandraDB to crash, because it can't use 
		// strings starting with '$' as JSON keys. Therefore, we'll cut off the '$' and log a warning.
		/*
		String contentType = entry.getContentType();
		if (contentType.charAt(0) == '$') {
			Logger.warn("Invalid content type found in log: " + contentType);
			contentType = contentType.substring(1);
		}
		knownHosts.incrementContentTypeCounter(hostname, contentType);
		
		String virusName = LogAnalytics.extractVirusName(entry);
		if (virusName != null) {
			knownHosts.incrementVirusStats(hostname, virusName);
			virusLog.recordOccurence(virusName, hostname);
		}
		*/
				
		// Step 5 - save
		// TODO optimize caching - insert LRU elements into DB when reasonable
		cache.put(timeslot, currentUnit);
	}

	private long toTimeslot(long timestamp) {
		 return (timestamp / CassandraProperties.PRE_AGGREGATION_RESOLUTION_MILLIS) * CassandraProperties.PRE_AGGREGATION_RESOLUTION_MILLIS;
	}
	
	public void commit() {
		for (CassandraCrawlStatsUnit csu : cache.values()) {
			csu.save(session);
		}
	}
	
}
