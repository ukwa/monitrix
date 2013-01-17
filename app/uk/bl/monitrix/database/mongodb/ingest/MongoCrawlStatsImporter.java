package uk.bl.monitrix.database.mongodb.ingest;

import java.util.HashMap;
import java.util.Map.Entry;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlStats;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlStatsUnit;
import uk.bl.monitrix.database.mongodb.model.MongoKnownHost;
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
class MongoCrawlStatsImporter extends MongoCrawlStats {
	
	private MongoKnownHostImporter knownHosts;
	
	private HashMap<KnownHost, Long> cachedHostCompletionTimes = new HashMap<KnownHost, Long>();
	
	public MongoCrawlStatsImporter(DB db, MongoKnownHostImporter knownHosts) {
		super(db);
		
		this.knownHosts = knownHosts;
	}
	
	/**
	 * Updates the crawl stats with a single log entry. Note that this method ONLY writes to
	 * the in-memory cache to avoid excessive DB transactions! To write to the DB, execute the
	 * .commit() method after your updates are done.
	 * @param entry the log entry
	 */
	public void update(CrawlLogEntry entry) {
		// Step 1 - compute the timeslot
		long timeslot = toTimeslot(entry.getTimestamp().getTime());
				
		// Step 2 - update data for this timeslot
		MongoCrawlStatsUnit dbo = (MongoCrawlStatsUnit) this.getStatsForTimestamp(timeslot);
		if (dbo == null) {
			// Step 3a - init data for this timeslot
			dbo = new MongoCrawlStatsUnit(new BasicDBObject());
			dbo.setTimestamp(timeslot);
			dbo.setNumberOfURLsCrawled(1);
			dbo.setDownloadVolume(entry.getDownloadSize());	
			dbo.setNumberOfNewHostsCrawled(0);
		} else {
			// Step 3b - update existing data for this timeslot
			dbo.setNumberOfURLsCrawled(dbo.getNumberOfURLsCrawled() + 1);
			dbo.setDownloadVolume(dbo.getDownloadVolume() + entry.getDownloadSize());
		}
		
		// Step 4 - update hosts info
		String hostname = entry.getHost();
		if (knownHosts.isKnown(hostname)) {
			KnownHost host = knownHosts.getKnownHost(hostname);
			
			if (!cachedHostCompletionTimes.containsKey(hostname))
				cachedHostCompletionTimes.put(host, host.getLastAccess());
			
			// Update last access time
			knownHosts.setLastAccess(hostname, entry.getTimestamp().getTime());
		} else {
			long timestamp = entry.getTimestamp().getTime();
			MongoKnownHost host = knownHosts.addToList(hostname, timestamp);
			dbo.setNumberOfNewHostsCrawled(dbo.getNumberOfNewHostsCrawled() + 1);
			dbo.setCompletedHosts(dbo.countCompletedHosts() + 1);
			cachedHostCompletionTimes.put(host, timestamp);
		}
		knownHosts.addSubdomain(hostname, entry.getSubdomain());
				
		// Step 5 - save
		// TODO optimize caching - insert LRU elements into DB when reasonable
		cache.put(timeslot, dbo);
	}
	
	private long toTimeslot(long timestamp) {
		 return (timestamp / MongoProperties.PRE_AGGREGATION_RESOLUTION_MILLIS) * MongoProperties.PRE_AGGREGATION_RESOLUTION_MILLIS;
	}
	
	/**
	 * Writes the contents of the cache to the database.
	 */
	public void commit() {
		for (Entry<KnownHost, Long> entry : cachedHostCompletionTimes.entrySet()) {
			MongoCrawlStatsUnit oldCompletionTimeslot = (MongoCrawlStatsUnit) getStatsForTimestamp(toTimeslot(entry.getValue()));
			MongoCrawlStatsUnit newCompletionTimeslot = (MongoCrawlStatsUnit) getStatsForTimestamp(toTimeslot(entry.getKey().getLastAccess()));
			
			if (oldCompletionTimeslot.getTimestamp() < newCompletionTimeslot.getTimestamp()) {
				oldCompletionTimeslot.setCompletedHosts(oldCompletionTimeslot.countCompletedHosts() - 1);
				newCompletionTimeslot.setCompletedHosts(newCompletionTimeslot.countCompletedHosts() + 1);
			}
		}
		
		// This means we're making individual commits to the DB
		// TODO see if we can optimize
		for (MongoCrawlStatsUnit dbo : cache.values()) {
			save(dbo);
		}
		
		cache.clear();
		cachedHostCompletionTimes.clear();
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
