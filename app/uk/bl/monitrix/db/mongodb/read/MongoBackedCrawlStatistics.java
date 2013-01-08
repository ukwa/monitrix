package uk.bl.monitrix.db.mongodb.read;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import uk.bl.monitrix.api.CrawlStatistics;
import uk.bl.monitrix.api.TimeseriesValue;
import uk.bl.monitrix.db.mongodb.model.GlobalStatsCollection;
import uk.bl.monitrix.db.mongodb.model.PreAggregatedStatsCollection;
import uk.bl.monitrix.db.mongodb.model.PreAggregatedStatsDBO;

/**
 * TODO needs caching!
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoBackedCrawlStatistics extends CrawlStatistics {
	
	private GlobalStatsCollection globalStats;
	
	private PreAggregatedStatsCollection preAggregatedStats;
	
	MongoBackedCrawlStatistics(GlobalStatsCollection globalStats, PreAggregatedStatsCollection preAggregatedStats) {
		this.globalStats = globalStats;
		this.preAggregatedStats = preAggregatedStats;
	}
	
	private List<PreAggregatedStatsDBO> getAggregatedStats(int maxDatapoints) {
		List<PreAggregatedStatsDBO> stats = new ArrayList<PreAggregatedStatsDBO>();

		Iterator<PreAggregatedStatsDBO> iterator = preAggregatedStats.getPreAggregatedStats();
		while (iterator.hasNext())
			stats.add(iterator.next());
		
		// Sort by time
		Collections.sort(stats);
		return stats;
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
	
	@Override
	public List<TimeseriesValue> getNewHostsCrawledHistory(int maxDatapoints) {
		final List<PreAggregatedStatsDBO> aggregatedStats = getAggregatedStats(maxDatapoints);
		
		return resample(new AbstractList<TimeseriesValue>() {
			@Override
			public TimeseriesValue get(int index) {
				PreAggregatedStatsDBO dbo = aggregatedStats.get(index);
				return new TimeseriesValue(dbo.getTimeslot(), dbo.getNumberOfNewHostsCrawled());
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
