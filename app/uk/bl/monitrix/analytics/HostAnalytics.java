package uk.bl.monitrix.analytics;

import java.util.Map;
import java.util.Map.Entry;

import uk.bl.monitrix.model.KnownHost;

public class HostAnalytics {
	
	// String constant - 'text' prefix for checking MIME types
	private static final String TEXT = "text";
	
	/**
	 * Extracts the percentage of redirect status codes observed at the specified host,
	 * using the fetch-status distribution table of the specified host. Redirects are
	 * considered everything with an HTTP status code of 3xx. 
	 * @param host the host
	 * @return the percentage of requests that received an HTTP 3xx repsonse
	 */
	public static double computePercentagOfRedirects(KnownHost host) {
		Map<String, Integer> statusDistribution = host.getFetchStatusDistribution();
		if (statusDistribution.size() == 0)
			return 0;
		
		double redirects = 0;
		for (Entry<String, Integer> entry : statusDistribution.entrySet()) {
			if (entry.getKey().startsWith("3"))
				redirects++;
		}
		
		return redirects / host.getCrawledURLs();
	}
	
	/**
	 * Extracts the percentage of HTTP requests that were precluded by robots.txt rules, using the 
	 * fetch-status distribution table of the specified host.
	 * According to http://crawler.archive.org/articles/user_manual/analysis.html, a block caused
	 * by robots.txt is indicated by a Heritrix fetch status code of -9998.
	 * @param host the host
	 * @return the percentage of requests blocked by robots.txt
	 */
	public static double computePercentageOfRobotsTxtBlocks(KnownHost host) {
		Map<String, Integer> statusDistribution = host.getFetchStatusDistribution();
		if (statusDistribution.size() == 0)
			return 0;
		
		Integer blocked = statusDistribution.get("-9998");
		if (blocked == null)
			return 0;
		
		return ((double) blocked.intValue()) / host.getCrawledURLs();
	}
	
	/**
	 * Extracts the text-to-nontext ratio from the fetch-status distribution table
	 * of the specified host.
	 * @param host the host
	 * @return the text-to-nontext ratio
	 */
	public static double computeTextToNonTextRatio(KnownHost host) {
		Map<String, Integer> statusDistribution = host.getFetchStatusDistribution();
		if (statusDistribution.size() == 0)
			return 0;
		
		double text = 0;
		for (Entry<String, Integer> entry : statusDistribution.entrySet()) {
			if (entry.getKey().startsWith(TEXT))
				text++;
		}

		return text / statusDistribution.size();
	}

}
