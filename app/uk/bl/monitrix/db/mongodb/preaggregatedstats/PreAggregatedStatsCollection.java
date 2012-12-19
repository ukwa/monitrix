package uk.bl.monitrix.db.mongodb.preaggregatedstats;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import uk.bl.monitrix.db.mongodb.MongoProperties;
import uk.bl.monitrix.db.mongodb.knownhosts.KnownHostsCollection;
import uk.bl.monitrix.heritrix.LogEntry;

/**
 * A wrapper around the 'Pre-Aggregated Stats' collection.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 *
 */
public class PreAggregatedStatsCollection {
	
	private DBCollection collection;
	
	private KnownHostsCollection knownHosts;
	
	private Map<Long, PreAggregatedStatsDBO> cache = new HashMap<Long, PreAggregatedStatsDBO>();
	
	public PreAggregatedStatsCollection(DB db, KnownHostsCollection knownHosts) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_PRE_AGGREGATED_STATS);
		
		// Collection is indexed by timeslot (will be skipped automatically if index exists)
		this.collection.createIndex(new BasicDBObject(MongoProperties.FIELD_PRE_AGGREGATED_TIMESLOT, 1));
		
		this.knownHosts = knownHosts;
	}
	
	public void save(PreAggregatedStatsDBO dbo) {
		this.collection.save(dbo.dbo);
	}
	
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
	
	public void update(LogEntry entry) {
		// Step 1 - compute the timeslot
		long timeslot = (entry.getTimestamp().getTime() / MongoProperties.PRE_AGGREGATION_RESOLUTION_MILLIS) *
				MongoProperties.PRE_AGGREGATION_RESOLUTION_MILLIS;
				
		// Step 2 - update timeline data for this timeslot
		PreAggregatedStatsDBO dbo = getStatsForTimeslot(timeslot);
		if (dbo == null) {
			// Step 3a - init pre-aggregated data for this timeslot
			dbo = new PreAggregatedStatsDBO(new BasicDBObject());
			dbo.setTimeslot(timeslot);
			dbo.setNumberOfURLs(1);
			dbo.setDownloadVolume(entry.getDownloadSize());	
			dbo.setNumberOfNewHostsCrawled(0);
		} else {
			// Step 3b - add to existing pre-aggregated data for this timeslot
			dbo.setNumberOfURLs(dbo.getNumberOfURLs() + 1);
			dbo.setDownloadVolume(dbo.getDownloadVolume() + entry.getDownloadSize());
		}
		
		// Step 4 - update known hosts info
		String hostname = entry.getHost();
		if (knownHosts.exists(hostname)) {
			// knownHosts.setLastAccess(hostname, entry.getTimestamp().getTime());
		} else {
			dbo.setNumberOfNewHostsCrawled(dbo.getNumberOfNewHostsCrawled() + 1);
			knownHosts.addToList(hostname, entry.getTimestamp().getTime());
		}

		// Step 5 - save
		// TODO optimize caching - insert LRU elements into DB when reasonable
		cache.put(timeslot, dbo);
	}
	
	public void commit() {
		// This means we're making individual commits to the DB
		// TODO optimize - make one query-based delete, followed by bulk insert!
		for (PreAggregatedStatsDBO dbo : cache.values()) {
			save(dbo);
		}
		cache.clear();
	}
	
	private PreAggregatedStatsDBO getStatsForTimeslot(long timeslot) {
		if (cache.containsKey(timeslot)) {
			return cache.get(timeslot);
		}
		
		DBObject query = new BasicDBObject(MongoProperties.FIELD_PRE_AGGREGATED_TIMESLOT, timeslot);
		DBObject result = collection.findOne(query);
		
		if (result == null)
			return null;
		else
			return new PreAggregatedStatsDBO(result);
	}

}
