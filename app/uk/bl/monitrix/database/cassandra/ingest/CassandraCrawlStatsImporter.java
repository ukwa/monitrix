package uk.bl.monitrix.database.cassandra.ingest;

import play.Logger;
import uk.bl.monitrix.analytics.LogAnalytics;
import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlStats;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlStatsUnit;
import uk.bl.monitrix.model.CrawlLogEntry;
import uk.bl.monitrix.model.KnownHost;

import com.mongodb.DB;
import com.mongodb.BasicDBObject;

/**
 * An extended version of {@link MongoCrawlStats} that adds ingest capability.
 * The ingest is 'smart' in the sense as it also performs various aggregation computations,
 * including those involving the known hosts list.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class CassandraCrawlStatsImporter extends MongoCrawlStats {
	
	private MongoKnownHostImporter knownHosts;
	
	private MongoVirusLogImporter virusLog;
	
	public CassandraCrawlStatsImporter(DB db, MongoKnownHostImporter knownHosts, MongoVirusLogImporter virusLog) {
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
	public void update(CrawlLogEntry entry) {
		// Step 1 - compute the timeslot
		long timeslot = toTimeslot(entry.getLogTimestamp().getTime());
				
		// Step 2 - update data for this timeslot
		MongoCrawlStatsUnit currentUnit = (MongoCrawlStatsUnit) getStatsForTimestamp(timeslot);
		if (currentUnit == null) {
			// Step 3a - init data for this timeslot
			currentUnit = new MongoCrawlStatsUnit(new BasicDBObject());
			currentUnit.setTimestamp(timeslot);
			currentUnit.setNumberOfURLsCrawled(1);
			currentUnit.setDownloadVolume(entry.getDownloadSize());	
			currentUnit.setNumberOfNewHostsCrawled(0);
		} else {
			// Step 3b - update existing data for this timeslot
			currentUnit.setNumberOfURLsCrawled(currentUnit.getNumberOfURLsCrawled() + 1);
			currentUnit.setDownloadVolume(currentUnit.getDownloadVolume() + entry.getDownloadSize());
		}
		
		// Step 4 - update hosts info
		String hostname = entry.getHost();
		if (knownHosts.isKnown(hostname)) {
			KnownHost host = knownHosts.getKnownHost(hostname);
			
			// Update host completion time
			long lastRecordedAccess = host.getLastAccess();
			if (lastRecordedAccess < timeslot) {
				MongoCrawlStatsUnit unitToModify = (MongoCrawlStatsUnit) getStatsForTimestamp(toTimeslot(lastRecordedAccess));
				unitToModify.setCompletedHosts(unitToModify.countCompletedHosts() - 1);
				currentUnit.setCompletedHosts(currentUnit.countCompletedHosts() + 1);
			}
			
			// Update last access time
			knownHosts.setLastAccess(hostname, entry.getLogTimestamp().getTime());
		} else {
			long timestamp = entry.getLogTimestamp().getTime();
			knownHosts.addToList(hostname, timestamp);
			currentUnit.setNumberOfNewHostsCrawled(currentUnit.getNumberOfNewHostsCrawled() + 1);
			currentUnit.setCompletedHosts(currentUnit.countCompletedHosts() + 1);
		}
		
		// Note: it's a little confusing that these aggregation steps are in this class
		// TODO move into the main MongoBatchImporter
		knownHosts.addSubdomain(hostname, entry.getSubdomain());
		knownHosts.incrementFetchStatusCounter(hostname, entry.getHTTPCode());
		knownHosts.incrementCrawledURLCounter(hostname);
		knownHosts.updateAverageResponseTimeAndRetryRate(hostname, entry.getFetchDuration(), entry.getRetries());
		
		// Warning: there seems to be a bug in Heritrix which sometimes leaves a 'content type template' (?)
		// in the log line: content type = '$ctype'. This causes MongoDB to crash, because it can't use 
		// strings starting with '$' as JSON keys. Therefore, we'll cut off the '$' and log a warning.
		String contentType = entry.getContentType();
		if (contentType.charAt(0) == '$') {
			Logger.warn("Invalid content type found in log: " + contentType);
			contentType = contentType.substring(1);
		}
		knownHosts.incrementContentTypeCounter(hostname, contentType);
		
		String virusName = LogAnalytics.extractVirusName(entry);
		if (virusName != null) {
			// MongoDB says: fields stored in the db can't have . in them.
			knownHosts.incrementVirusStats(hostname, virusName.replace('.', '@'));
			virusLog.recordOccurence(virusName, hostname);
		}
				
		// Step 5 - save
		// TODO optimize caching - insert LRU elements into DB when reasonable
		cache.put(timeslot, currentUnit);
	}
	
	private long toTimeslot(long timestamp) {
		 return (timestamp / MongoProperties.PRE_AGGREGATION_RESOLUTION_MILLIS) * MongoProperties.PRE_AGGREGATION_RESOLUTION_MILLIS;
	}
	
	/**
	 * Writes the contents of the cache to the database.
	 */
	public void commit() {
		// This means we're making individual commits to the DB
		// TODO see if we can optimize
		for (MongoCrawlStatsUnit dbo : cache.values()) {
			save(dbo);
		}
		
		cache.clear();
		knownHosts.commit();
	}

	/**
	 * Saves the wrapped DBObject to the collection.
	 * @param dbo the wrapped DBObject
	 */
	public void save(MongoCrawlStatsUnit unit) {
		collection.save(unit.getBackingDBO());
	}
	
}
