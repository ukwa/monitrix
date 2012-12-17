package uk.bl.monitrix.db.mongodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import uk.bl.monitrix.CrawlStatistics;
import uk.bl.monitrix.TimeseriesValue;

/**
 * A FIRST DUMMY implementation of {@link CrawlStatistics} backed by MongoDB. Currently (mostly) a dummy.
 * 
 * TODO replace with a real, decent implementation
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoBackedCrawlStatistics implements CrawlStatistics {
	
	private long crawlStartTime;
	
	private long crawlLastActivity;
	
	private DBCollection preAggregatedStats;
	
	MongoBackedCrawlStatistics(DB db) {
		preAggregatedStats = db.getCollection(MongoProperties.COLLECTION_PRE_AGGREGATED_STATS);
		DBCollection globalStats = db.getCollection(MongoProperties.COLLECTION_GLOBAL_STATS);
		DBObject stats = null;
		
		DBCursor cursor = globalStats.find();
		if (cursor.hasNext())
			stats = cursor.next();
		cursor.close();
		
		if (stats == null)
			throw new RuntimeException("Corrupt DB - Global crawl stats missing!");
		
		this.crawlStartTime = (Long) stats.get(MongoProperties.FIELD_GLOBAL_CRAWL_START);
		this.crawlLastActivity = (Long) stats.get(MongoProperties.FIELD_GLOBAL_CRAWL_LAST_ACTIVITIY); 
	}

	@Override
	public long getCrawlStartTime() {
		return crawlStartTime;
	}

	@Override
	public long getTimeOfLastCrawlActivity() {
		return this.crawlLastActivity;
	}

	@Override
	public List<TimeseriesValue> getDatavolumeHistory() {
		List<TimeseriesValue> series = new ArrayList<TimeseriesValue>();
		DBCursor cursor = preAggregatedStats.find();
		while (cursor.hasNext()) {
			DBObject next = cursor.next();
			Long timestamp = (Long) next.get(MongoProperties.FIELD_PRE_AGGREGATED_TIMESLOT);
			Integer downloadVolume = (Integer) next.get(MongoProperties.FIELD_PRE_AGGREGATED_DOWNLOAD_VOLUME);
			series.add(new TimeseriesValue(timestamp.longValue(), downloadVolume.longValue()));
		}
		
		Collections.sort(series);
		System.out.println("got " + series.size() + " records");
		// List<TimeseriesValue> collapsed = collapse(series.iterator(), 20);
		//System.out.println("collapsed to " + collapsed.size());
		//return collapsed;
		return series;
	}
	
	private List<TimeseriesValue> collapse(Iterator<TimeseriesValue> series, int factor) {
		List<TimeseriesValue> collapsed = new ArrayList<TimeseriesValue>();
		
		while (series.hasNext()) {
			TimeseriesValue next = series.next();
			
			int counter = 0;
			long timestamp = next.getTimestamp();
			long aggregatedValue = next.getValue();
			while (series.hasNext() && counter < (factor - 1)) {
				next = series.next();
				aggregatedValue += next.getValue();
				counter++;
			}
						
			collapsed.add(new TimeseriesValue(timestamp, aggregatedValue));
		}
		
		return collapsed;
	}

	@Override
	public List<TimeseriesValue> getCrawledURLsHistory() {
		// TODO Auto-generated method stub
		return null;
	}

}
