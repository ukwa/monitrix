package uk.bl.monitrix.db.mongodb.model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import uk.bl.monitrix.api.CrawlLogEntry;
import uk.bl.monitrix.db.mongodb.MongoProperties;

/**
 * Wraps the MongoDB 'Pre-Aggregated Stats' collection.
 * 
 * TODO improve caching!
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class PreAggregatedStatsCollection {
	
	private DBCollection collection;
	
	private KnownHostsCollection knownHosts;
	
	// A simple in-memory buffer for quick stats lookups
	private Map<Long, PreAggregatedStatsDBO> cache = new HashMap<Long, PreAggregatedStatsDBO>();
	
	public PreAggregatedStatsCollection(DB db, KnownHostsCollection knownHosts) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_PRE_AGGREGATED_STATS);		
		this.knownHosts = knownHosts;
		
		// Collection is indexed by timeslot (will be skipped automatically if index exists)
		this.collection.createIndex(new BasicDBObject(MongoProperties.FIELD_PRE_AGGREGATED_TIMESLOT, 1));
	}
	
	/**
	 * Saves the wrapped DBObject to the collection.
	 * @param dbo the wrapped DBObject
	 */
	public void save(PreAggregatedStatsDBO dbo) {
		collection.save(dbo.dbo);
	}
	
	/**
	 * Returns all pre-aggregated stats from the database.
	 * @return all pre-aggregated stats
	 */
	public Iterator<PreAggregatedStatsDBO> getPreAggregatedStats() {
		final DBCursor cursor = collection.find();
		return new Iterator<PreAggregatedStatsDBO>() {
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public PreAggregatedStatsDBO next() {
				return new PreAggregatedStatsDBO(cursor.next());
			}

			@Override
			public void remove() {
				cursor.remove();	
			}
		};
	}
	
	/**
	 * Updates the pre-aggregated stats with a single log entry. Note that this method ONLY writes to
	 * the IN-MEMORY CACHE! In order to write to the database, execute the .commit() method after your
	 * updates are done.
	 * @param entry the log entry
	 */
	public void update(CrawlLogEntry entry) {
		// Step 1 - compute the timeslot
		long timeslot = (entry.getTimestamp().getTime() / MongoProperties.PRE_AGGREGATION_RESOLUTION_MILLIS) *
				MongoProperties.PRE_AGGREGATION_RESOLUTION_MILLIS;
				
		// Step 2 - update data for this timeslot
		PreAggregatedStatsDBO dbo = getStatsForTimeslot(timeslot);
		if (dbo == null) {
			// Step 3a - init data for this timeslot
			dbo = new PreAggregatedStatsDBO(new BasicDBObject());
			dbo.setTimeslot(timeslot);
			dbo.setNumberOfURLs(1);
			dbo.setDownloadVolume(entry.getDownloadSize());	
			dbo.setNumberOfNewHostsCrawled(0);
		} else {
			// Step 3b - update existing data for this timeslot
			dbo.setNumberOfURLs(dbo.getNumberOfURLs() + 1);
			dbo.setDownloadVolume(dbo.getDownloadVolume() + entry.getDownloadSize());
		}
		
		// Step 4 - update hosts info
		String hostname = entry.getHost();
		if (knownHosts.exists(hostname)) {
			knownHosts.setLastAccess(hostname, entry.getTimestamp().getTime());
		} else {
			dbo.setNumberOfNewHostsCrawled(dbo.getNumberOfNewHostsCrawled() + 1);
			knownHosts.addToList(hostname, entry.getTimestamp().getTime());
		}
		
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
		for (PreAggregatedStatsDBO dbo : cache.values()) {
			save(dbo);
		}
		cache.clear();
		knownHosts.commit();
	}
	
	/**
	 * Returns the pre-aggregated stats for a specific timeslot. To minimize database
	 * access, this method will first check against an in-memory cache, and only against
	 * the database if the memory cache yielded no hit.
	 * @param timeslot the timeslot
	 * @return the pre-aggregated stats
	 */
	private PreAggregatedStatsDBO getStatsForTimeslot(long timeslot) {
		if (cache.containsKey(timeslot))
			return cache.get(timeslot);
		
		DBObject query = new BasicDBObject(MongoProperties.FIELD_PRE_AGGREGATED_TIMESLOT, timeslot);
		DBObject result = collection.findOne(query);
		
		if (result == null) {
			return null;
		} else {
			PreAggregatedStatsDBO dbo = new PreAggregatedStatsDBO(result);
			cache.put(timeslot, dbo);
			return dbo;
		}
	}

}
