package uk.bl.monitrix.database.mongodb.ingest;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlStats;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlStatsUnit;
import uk.bl.monitrix.model.CrawlLogEntry;

import com.mongodb.DB;
import com.mongodb.BasicDBObject;

class MongoCrawlStatsImporter extends MongoCrawlStats {
	
	private MongoKnownHostImporter knownHosts;
	
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
		long timeslot = (entry.getTimestamp().getTime() / MongoProperties.PRE_AGGREGATION_RESOLUTION_MILLIS) *
				MongoProperties.PRE_AGGREGATION_RESOLUTION_MILLIS;
				
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
			knownHosts.setLastAccess(hostname, entry.getTimestamp().getTime());
		} else {
			dbo.setNumberOfNewHostsCrawled(dbo.getNumberOfNewHostsCrawled() + 1);
			knownHosts.addToList(hostname, entry.getTimestamp().getTime());
		}
		knownHosts.addSubdomain(hostname, entry.getSubdomain());
		
		// Step 5 - save
		// TODO optimize caching - insert LRU elements into DB when reasonable
		cache.put(timeslot, dbo);
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
