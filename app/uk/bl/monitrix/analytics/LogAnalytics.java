package uk.bl.monitrix.analytics;

import java.util.ArrayList;
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
 * @author Rainer Simon <rainer.simon@ait.ac.a.t>
 */
public class LogAnalytics {
	
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
		Logger.info("Extracted Crawler IDs - took " + (System.currentTimeMillis() - computeStart));
		return sorted;
	}
	
	/**
	 * Computes the average crawl rate values (i.e. average number of URLs crawled per minute)
	 * for the given log entries.
	 * @param log the log entries
	 * @return average download crawl rate in URLs/minute
	 */
	public static long getAverageCrawlRate(Iterator<CrawlLogEntry> log) {
		long computeStart = System.currentTimeMillis();
		
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
		
		Logger.info("Computed average crawl rate - took " + (System.currentTimeMillis() - computeStart));
		return Math.round(urlsPerMillisecond * 60000);
	}
	
	/**
	 * Computes the average download rate value (in MB/minute) for the given log entries.
	 * @param log the log entries
	 * @return average download rate in MB/minute
	 */	
	public static long getAverageDownloadRate(Iterator<CrawlLogEntry> log) {
		long computeStart = System.currentTimeMillis();
		
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
		
		Logger.info("Computed average download rate - took " + (System.currentTimeMillis() - computeStart));
		return Math.round(bytesPerMillisecond * 60000);
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
		
		Logger.info("Computed fetch status distribution - took " + (System.currentTimeMillis() - computeStart));
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
		
		Logger.info("Computed MIME type distribution - took " + (System.currentTimeMillis() - computeStart));
		return pieChart;
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
		
		Logger.info("Computed virus distribution - took " + (System.currentTimeMillis() - computeStart));
		return pieChart;
	}

}
