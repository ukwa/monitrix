package uk.bl.monitrix.analytics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import play.Logger;

import uk.bl.monitrix.model.CrawlLogEntry;

/**
 * Helper functions for computing various stats from log entries. 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class LogAnalytics {
	
	// String constant - 'text' prefix for checking MIME types
	private static final String TEXT = "text";
	
	// Virus flag 'FOUND'
	private static final String FOUND = "FOUND";
	
	/**
	 * Extracts the unique crawler IDs from the log entries. 
	 * @param log the log entries
	 * @return the IDs of the crawlers that crawled this host
	 */
	public static List<String> getCrawlerIDs(Iterator<CrawlLogEntry> log) {
		long computeStart = System.currentTimeMillis();
		Set<String> crawlers = new HashSet<String>();

		while (log.hasNext())
			crawlers.add(log.next().getCrawlerID());
		
		List<String> sorted = new ArrayList<String>(crawlers);
		Collections.sort(sorted);
		Logger.info("Extracted Crawler IDs - took " + (System.currentTimeMillis() - computeStart) + "ms");
		return sorted;
	}
	
	/**
	 * Computes the average crawl rate values (i.e. average number of URLs crawled per minute)
	 * for the given log entries.
	 * @param log the log entries
	 * @return average download crawl rate in URLs/minute
	 */
	public static long getAverageCrawlRate(Iterator<CrawlLogEntry> log) {		
		long startTime = Long.MAX_VALUE;
		long endTime = Long.MIN_VALUE;
		long totalURLs = 0;
		while (log.hasNext()) {
			CrawlLogEntry next = log.next();
			
			long timestamp = next.getTimestamp().getTime();
			if (timestamp > endTime)
				endTime = timestamp;
			
			if (timestamp < startTime)
				startTime = timestamp;
			
			totalURLs++;
		}
		
		double urlsPerMillisecond = ((double) totalURLs) / ((double) (endTime - startTime));
		
		return Math.round(urlsPerMillisecond * 60000);
	}
	
	/**
	 * Computes the average download rate value (in MB/minute) for the given log entries.
	 * @param log the log entries
	 * @return average download rate in MB/minute
	 */	
	public static long getAverageDownloadRate(Iterator<CrawlLogEntry> log) {		
		long startTime = Long.MAX_VALUE;
		long endTime = Long.MIN_VALUE;
		long downloadVolume = 0;
		while (log.hasNext()) {
			CrawlLogEntry next = log.next();
			
			long timestamp = next.getTimestamp().getTime();
			if (timestamp > endTime)
				endTime = timestamp;
			
			if (timestamp < startTime)
				startTime = timestamp;
			
			downloadVolume += next.getDownloadSize();
		}
		
		double bytesPerMillisecond = ((double) downloadVolume) / ((double) (endTime - startTime));
		
		return Math.round(bytesPerMillisecond * 60000);
	}
	
	public static String extractVirusName(CrawlLogEntry entry) {
		String annotations = entry.getAnnotations();
		if (annotations.contains(FOUND)) {
			// Example: 84.45.47.58,1: stream: JS.Redir-11 FOUND
			String virusName = annotations.substring(0, annotations.indexOf(FOUND)).trim();
			virusName = virusName.substring(virusName.lastIndexOf(' ') + 1);
			return virusName;
		}
		
		return null;
	}
	
	/**
	 * Computes the distribution of fetch status codes in the log entries.
	 * @param log the log entries
	 * @return the distribution of fetch status codes
	 */
	public static List<PieChartValue> getFetchStatusDistribution(Iterator<CrawlLogEntry> log) {
		long computeStart = System.currentTimeMillis();
		
		Map<Integer, Integer> codes = new HashMap<Integer, Integer>();
		
		while (log.hasNext()) {			
			Integer code = log.next().getHTTPCode();
			Integer value = codes.get(code);
			
			if (value == null)
				value = Integer.valueOf(1);
			else
				value = Integer.valueOf(value.intValue() + 1);
			
			codes.put(code, value);
		}
		
		List<PieChartValue> pieChart = new ArrayList<PieChartValue>();
		for (Entry<Integer, Integer> entry : codes.entrySet())
			pieChart.add(new PieChartValue(entry.getKey().toString(), entry.getValue()));
		
		Logger.info("Computed fetch status distribution - took " + (System.currentTimeMillis() - computeStart) + "ms");
		return pieChart;
	}
	
	/**
	 * Computes the distribution of MIME types from the log entries.
	 * @param log the log entries
	 * @return the distribution of MIME types
	 */
	public static List<PieChartValue> getMimeTypeDistribution(Iterator<CrawlLogEntry> log) {
		long computeStart = System.currentTimeMillis();
		
		Map<String, Integer> mimeTypes = new HashMap<String, Integer>();
		
		while (log.hasNext()) {		
			String mime = log.next().getContentType();
			Integer count = mimeTypes.get(mime);

			if (count == null)
				count = Integer.valueOf(1);
			else
				count = Integer.valueOf(count.intValue() + 1);
			
			mimeTypes.put(mime, count);
		}
		
		List<PieChartValue> pieChart = new ArrayList<PieChartValue>();
		for (Entry<String, Integer> entry : mimeTypes.entrySet())
			pieChart.add(new PieChartValue(entry.getKey(), entry.getValue()));
		
		Logger.info("Computed MIME type distribution - took " + (System.currentTimeMillis() - computeStart) + "ms");
		return pieChart;
	}
	
	/**
	 * Helper function to compute the ratio of text MIME types (i.e. every type starting with 'text/') vs.
	 * all other types.
	 * @param log the log entries
	 * @return the ratio text/non-text resources
	 */
	public static double getTextToNonTextResourceRatio(Iterator<CrawlLogEntry> log) {
		int text = 0;
		int nonText = 0;
		
		while (log.hasNext()) {			
			String contentType = log.next().getContentType();
			if (contentType.startsWith(TEXT))
				text++;
			else
				nonText++;
		}
		
		if (nonText == 0)
			return 99;
		
		return ((double) text)/((double) nonText);
	}
	
	/**
	 * Computes the distribution of clean vs. virused URLs from the log entries. 
	 * @param log the log entries
	 * @return the virus distribution
	 */
	public static List<PieChartValue> getVirusDistribution(Iterator<CrawlLogEntry> log) {
		long computeStart = System.currentTimeMillis();
		
		// TODO dummy impl only - improve so that viruses are recorded by name
		int clean = 0;
		int infected = 0;
		
		while (log.hasNext()) {			
			String annotations = log.next().getAnnotations();
			if (annotations.contains("FOUND")) {
				infected++;
			} else {
				clean++;
			}
		}
		
		List<PieChartValue> pieChart = new ArrayList<PieChartValue>();
		pieChart.add(new PieChartValue("Clean", clean));
		if (infected > 0)
			pieChart.add(new PieChartValue("Infected", infected));
		
		Logger.info("Computed virus distribution - took " + (System.currentTimeMillis() - computeStart) + "ms");
		return pieChart;
	}
	
	public static List<TimeseriesValue> getCrawledURLsHistory(Collection<CrawlLogEntry> log, int maxDatapoints) {
		Logger.info("Computing URL history timeseries for " + log.size() + " log entries");
		
		// Get log start and end time
		long logStartTime = Long.MAX_VALUE;
		long logEndTime = 0;
		for (CrawlLogEntry entry : log) {
			long timestamp = entry.getTimestamp().getTime();
			
			if (timestamp > logEndTime)
				logEndTime = timestamp;
			
			if (timestamp < logStartTime)
				logStartTime = timestamp;
		}
		
		// Compute timeseries resolution (= # of millis in one data point bucket)
		long resolution = (logEndTime - logStartTime) / maxDatapoints;
		
		// (timeslot -> # of URLs)
		Map<Long, TimeseriesValue> graph = new HashMap<Long, TimeseriesValue>(maxDatapoints);
		
		for (CrawlLogEntry entry : log) {
			long timeslot = entry.getTimestamp().getTime() / resolution;
			
			TimeseriesValue urlCount = graph.get(timeslot);
			if (urlCount == null)
				graph.put(timeslot, new TimeseriesValue(timeslot * resolution, 1));
			else
				urlCount.setValue(urlCount.getValue() + 1);
		}
				
		List<TimeseriesValue> timeseries = new ArrayList<TimeseriesValue>(graph.values());
		Collections.sort(timeseries);
		
		Logger.info("Done.");
		return timeseries;
	}

}
