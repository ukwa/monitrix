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
		Set<String> crawlers = new HashSet<String>();

		while (log.hasNext())
			crawlers.add(log.next().getCrawlerID());
		
		List<String> sorted = new ArrayList<String>(crawlers);
		Collections.sort(sorted);
		return sorted;
	}
	
	/**
	 * Computes the distribution of fetch status codes in the log entries.
	 * @param log the log entries
	 * @return the distribution of fetch status codes
	 */
	public static List<PieChartValue> getFetchStatusDistribution(Iterator<CrawlLogEntry> log) {
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
		
		return pieChart;
	}
	
	/**
	 * Computes the distribution of MIME types from the log entries.
	 * @param log the log entries
	 * @return the distribution of MIME types
	 */
	public static List<PieChartValue> getMimeTypeDistribution(Iterator<CrawlLogEntry> log) {
		// TODO implement
		return null;
	}
	
	/**
	 * Computes the distribution of clean vs. virused URLs from the log entries. 
	 * @param log the log entries
	 * @return the virus distribution
	 */
	public static List<PieChartValue> getVirusDistribution(Iterator<CrawlLogEntry> log) {
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
		pieChart.add(new PieChartValue("Infected", infected));
		return pieChart;
	}

}
