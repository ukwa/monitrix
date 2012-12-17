package uk.bl.monitrix.db.mongodb;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.mongodb.DB;

import uk.bl.monitrix.CrawlStatistics;
import uk.bl.monitrix.TimeseriesValue;
import uk.bl.monitrix.db.mongodb.globalstats.GlobalStatsCollection;
import uk.bl.monitrix.db.mongodb.preaggregatedstats.PreAggregatedStatsCollection;
import uk.bl.monitrix.db.mongodb.preaggregatedstats.PreAggregatedStatsDBO;

/**
 * TODO this class needs caching.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoBackedCrawlStatistics implements CrawlStatistics {
	
	/** Duration after which memory-cached stats are considered invalid **/
	private static final int CACHE_TIMEOUT_MILLIS = 2 * 60 * 1000;
	
	private GlobalStatsCollection globalStats;
	
	private PreAggregatedStatsCollection preAggregatedStats;
	
	private List<PreAggregatedStatsDBO> statsCache = new ArrayList<PreAggregatedStatsDBO>();
	
	private long lastStatsCacheAccess = 0;
	
	MongoBackedCrawlStatistics(DB db) {
		globalStats = new GlobalStatsCollection(db);
		preAggregatedStats = new PreAggregatedStatsCollection(db);		
	}
	
	private List<PreAggregatedStatsDBO> getAggregatedStats(int maxDatapoints) {
		if (System.currentTimeMillis() > lastStatsCacheAccess + CACHE_TIMEOUT_MILLIS) {
			// Clear cache
			statsCache.clear();
			
			// Read from DB
			Iterator<PreAggregatedStatsDBO> iterator = preAggregatedStats.getPreAggregatedStats();
			while (iterator.hasNext())
				statsCache.add(iterator.next());
			
			// Sort by time
			Collections.sort(statsCache);
		}
		
		return statsCache;
	}

	@Override
	public long getCrawlStartTime() {
		// TODO cache
		return globalStats.getStats().getCrawlStartTime();
	}

	@Override
	public long getTimeOfLastCrawlActivity() {
		// TODO cache
		return globalStats.getStats().getCrawlLastActivity();
	}

	@Override
	public List<TimeseriesValue> getDatavolumeHistory(int maxDatapoints) {
		final List<PreAggregatedStatsDBO> aggregatedStats = getAggregatedStats(maxDatapoints);
		
		return resample(new AbstractList<TimeseriesValue>() {
			@Override
			public TimeseriesValue get(int index) {
				PreAggregatedStatsDBO dbo = aggregatedStats.get(index);
				return new TimeseriesValue(dbo.getTimeslot(), dbo.getDownloadVolume());
			}

			@Override
			public int size() {
				return aggregatedStats.size();
			}
		}, aggregatedStats.size() / maxDatapoints);
	}
	
	@Override
	public List<TimeseriesValue> getCrawledURLsHistory(int maxDatapoints) {
		final List<PreAggregatedStatsDBO> aggregatedStats = getAggregatedStats(maxDatapoints);
		
		return resample(new AbstractList<TimeseriesValue>() {
			@Override
			public TimeseriesValue get(int index) {
				PreAggregatedStatsDBO dbo = aggregatedStats.get(index);
				return new TimeseriesValue(dbo.getTimeslot(), dbo.getNumberOfURLs());
			}

			@Override
			public int size() {
				return aggregatedStats.size();
			}
		}, aggregatedStats.size() / maxDatapoints);
	}	
	
	private List<TimeseriesValue> resample(List<TimeseriesValue> series, int factor) {
		Iterator<TimeseriesValue> original = series.iterator();
		
		List<TimeseriesValue> resampled = new ArrayList<TimeseriesValue>();
		while (original.hasNext()) {
			TimeseriesValue next = original.next();
			
			int counter = 0;
			long timestamp = next.getTimestamp();
			long aggregatedValue = next.getValue();
			while (original.hasNext() && counter < (factor - 1)) {
				next = original.next();
				aggregatedValue += next.getValue();
				counter++;
			}
						
			resampled.add(new TimeseriesValue(timestamp, aggregatedValue));
		}
		
		return resampled;
	}


}
