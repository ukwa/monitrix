package uk.bl.monitrix.database.cassandra.ingest;

import com.datastax.driver.core.Session;

import play.Logger;
import uk.bl.monitrix.analytics.LogAnalytics;
import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.database.cassandra.model.CassandraCrawlStats;
import uk.bl.monitrix.database.cassandra.model.CassandraCrawlStatsUnit;
import uk.bl.monitrix.model.CrawlLogEntry;
import uk.bl.monitrix.model.KnownHost;

/**
 * An extended version of {@link CassandraCrawlStats} that adds ingest capability.
 * The ingest is 'smart' in the sense as it also performs various aggregation computations,
 * including those involving the known hosts list.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class CassandraCrawlStatsImporter extends CassandraCrawlStats {
	
	private CassandraKnownHostImporter knownHosts;
	
	private CassandraVirusLogImporter virusLog;
	
	public CassandraCrawlStatsImporter(Session db, CassandraKnownHostImporter knownHosts, CassandraVirusLogImporter virusLog) {
		super(db);
		
		this.knownHosts = knownHosts;
		this.virusLog = virusLog;
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
		session.execute("UPDATE crawl_uris.stats SET uris_crawled = uris_crawled + 1, downloaded_bytes = downloaded_bytes + " + 
			entry.getDownloadSize() + whereClause(timeslot,crawl_id) + ";");
		
		// Step 4 - update hosts info
		String hostname = entry.getHost();
		if (knownHosts.isKnown(hostname)) {
			KnownHost host = knownHosts.getKnownHost(hostname);
			
			// Update host completion time
			long lastRecordedAccess = host.getLastAccess();
			if (lastRecordedAccess < timeslot) {
				long lrtimeslot = toTimeslot(lastRecordedAccess);
//				CassandraCrawlStatsUnit unitToModify = (CassandraCrawlStatsUnit) getStatsForTimestamp(lrtimeslot);
				session.execute("UPDATE crawl_uris.stats SET completed_hosts = completed_hosts - 1" + whereClause(lrtimeslot,crawl_id));
//				unitToModify.setCompletedHosts(unitToModify.countCompletedHosts() - 1);
				session.execute("UPDATE crawl_uris.stats SET completed_hosts = completed_hosts + 1" + whereClause(timeslot,crawl_id));
//				currentUnit.setCompletedHosts(currentUnit.countCompletedHosts() + 1);
			}
			
			// Update last access time
			knownHosts.setLastAccess(hostname, entry.getLogTimestamp().getTime());
		} else {
			long timestamp = entry.getLogTimestamp().getTime();
			knownHosts.addToList(hostname, entry.getDomain(), entry.getSubdomain(), timestamp);
			session.execute("UPDATE crawl_uris.stats SET new_hosts = new_hosts + 1" + whereClause(timeslot,crawl_id));
//			currentUnit.setNumberOfNewHostsCrawled(currentUnit.getNumberOfNewHostsCrawled() + 1);
			session.execute("UPDATE crawl_uris.stats SET completed_hosts = completed_hosts + 1" + whereClause(timeslot,crawl_id));
//			currentUnit.setCompletedHosts(currentUnit.countCompletedHosts() + 1);
		}
		
		// Note: it's a little confusing that these aggregation steps are in this class
		// TODO move into the main CassandraBatchImporter
		knownHosts.incrementFetchStatusCounter(hostname, entry.getHTTPCode());
		knownHosts.incrementCrawledURLCounter(hostname);
		knownHosts.updateAverageResponseTimeAndRetryRate(hostname, entry.getFetchDuration(), entry.getRetries());
		
		// Warning: there seems to be a bug in Heritrix which sometimes leaves a 'content type template' (?)
		// in the log line: content type = '$ctype'. This causes CassandraDB to crash, because it can't use 
		// strings starting with '$' as JSON keys. Therefore, we'll cut off the '$' and log a warning.
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
				
		// Step 5 - save
		// TODO optimize caching - insert LRU elements into DB when reasonable
		cache.put(timeslot, currentUnit);
	}
	
	private String whereClause(long timeslot, String crawl_id) {
	    return " WHERE stat_ts="+timeslot+" AND crawl_id='"+crawl_id+"';";
	}
	
	private long toTimeslot(long timestamp) {
		 return (timestamp / CassandraProperties.PRE_AGGREGATION_RESOLUTION_MILLIS) * CassandraProperties.PRE_AGGREGATION_RESOLUTION_MILLIS;
	}
	
}
