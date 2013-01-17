package uk.bl.monitrix.analytics;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import uk.bl.monitrix.model.CrawlStatsUnit;

/**
 * Helper functions for computing various stats from CrawlStats collection. 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CrawlStatsAnalytics {
	
	/**
	 * Extracts the datavolume timeseries (i.e. the amount of data that has been downloaded over time)
	 * from the provided crawl stats. The resulting timeseries will be resampled from its original
	 * resolution, so that the result has at most <code>maxDatapoints</code>
	 * data points.  
	 * @param crawlStats the source crawl stats
	 * @param maxDatapoints the maximum number of datapoints the timeseries should have
	 * @return the timeseries
	 */
	public static List<TimeseriesValue> getDatavolumeHistory(final List<CrawlStatsUnit> crawlStats, int maxDatapoints) {			
		return resample(new AbstractList<TimeseriesValue>() {
			@Override
			public TimeseriesValue get(int index) {
				CrawlStatsUnit unit = crawlStats.get(index);
				return new TimeseriesValue(unit.getTimestamp(), unit.getDownloadVolume());
			}

			@Override
			public int size() {
				return crawlStats.size();
			}
		}, crawlStats.size() / maxDatapoints);
	}
	
	public static List<TimeseriesValue> getDatavolumeHistory(Iterator<CrawlStatsUnit> crawlStats, int maxDatapoints) {
		return getDatavolumeHistory(toList(crawlStats), maxDatapoints);
	}
	
	/**
	 * Extracts the crawled URLs timeseries (i.e. the number of URLs visited over time) from the provided
	 * crawl stats. The resulting timeseries will be resampled from its original resolution, so that the
	 * result has at most <code>maxDatapoints</code> data points.  
	 * @param crawlStats the source crawl stats
	 * @param maxDatapoints the maximum number of datapoints the timeseries should have
	 * @return the timeseries
	 */
	public static List<TimeseriesValue> getCrawledURLsHistory(final List<CrawlStatsUnit> crawlStats, int maxDatapoints) {		
		return resample(new AbstractList<TimeseriesValue>() {
			@Override
			public TimeseriesValue get(int index) {
				CrawlStatsUnit unit = crawlStats.get(index);
				return new TimeseriesValue(unit.getTimestamp(), unit.getNumberOfURLsCrawled());
			}

			@Override
			public int size() {
				return crawlStats.size();
			}
		}, crawlStats.size() / maxDatapoints);		
	}
	
	public static List<TimeseriesValue> getCrawledURLsHistory(Iterator<CrawlStatsUnit> crawlStats, int maxDatapoints) {
		return getCrawledURLsHistory(toList(crawlStats), maxDatapoints);
	}
	
	/**
	 * Extracts the new hosts timeseries (i.e. the number of hosts that were visited for the 
	 * first time over the duration of the crawl. The timeseries will be resampled from its
	 * internal base resolution, so that the result has at most <code>maxDatapoints</code> 
	 * data points.  
	 * @param crawlStats the source crawl stats
	 * @param maxDatapoints the maximum number of datapoints the timeseries should have
	 * @return
	 */
	public static List<TimeseriesValue> getNewHostsCrawledHistory(final List<CrawlStatsUnit> crawlStats, int maxDatapoints) {		
		return resample(new AbstractList<TimeseriesValue>() {
			@Override
			public TimeseriesValue get(int index) {
				CrawlStatsUnit unit = crawlStats.get(index);
				return new TimeseriesValue(unit.getTimestamp(), unit.getNumberOfNewHostsCrawled());
			}

			@Override
			public int size() {
				return crawlStats.size();
			}
		}, crawlStats.size() / maxDatapoints);		
	}
	
	public static List<TimeseriesValue> getNewHostsCrawledHistory(Iterator<CrawlStatsUnit> crawlStats, int maxDatapoints) {
		return getNewHostsCrawledHistory(toList(crawlStats), maxDatapoints);
	}
	
	public static List<TimeseriesValue> getCompletedHostsHistory(final List<CrawlStatsUnit> crawlStats, int maxDatapoints) {
		return resample(new AbstractList<TimeseriesValue>() {
			@Override
			public TimeseriesValue get(int index) {
				CrawlStatsUnit unit = crawlStats.get(index);
				return new TimeseriesValue(unit.getTimestamp(), unit.countCompletedHosts());
			}

			@Override
			public int size() {
				return crawlStats.size();
			}
		}, crawlStats.size() / maxDatapoints);	
	}
	
	public static List<TimeseriesValue> getCompletedHostsHistory(Iterator<CrawlStatsUnit> crawlStats, int maxDatapoints) {
		return getCompletedHostsHistory(toList(crawlStats), maxDatapoints);
	}
	
	private static List<TimeseriesValue> resample(List<TimeseriesValue> series, int factor) {
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
	
	private static List<CrawlStatsUnit> toList(Iterator<CrawlStatsUnit> iterator) {
		List<CrawlStatsUnit> list = new ArrayList<CrawlStatsUnit>();
		
		while (iterator.hasNext())
			list.add(iterator.next());
		
		return list;
	}
	
}
