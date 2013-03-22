package uk.bl.monitrix.model;

import java.util.List;
import java.util.Map;

/**
 * The Known Host domain object interface. Encapsulates the information collected about a specific host
 * during the crawl.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public abstract class KnownHost {
	
	/**
	 * Host name.
	 * @return the host name
	 */
	public abstract String getHostname();
	
	/**
	 * The top level domain (e.g. "com" or "uk")
	 * @return the top level domain
	 */
	public abstract String getTopLevelDomain();
	
	/**
	 * The list of subdomains encountered during the crawl.
	 * @return the subdomains
	 */
	public abstract List<String> getSubdomains();
	
	/**
	 * UNIX timestamp of the first recorded access to this host.
	 * @return
	 */
	public abstract long getFirstAccess();
	
	/**
	 * UNIX timestamp of the last recorded access to this host.
	 * @return the last access to the host
	 */
	public abstract long getLastAccess();
	
	/**
	 * The list of crawlers that have been crawling this host.
	 * @return the list of crawler IDs
	 */
	public abstract List<String> getCrawlerIDs();
	
	/**
	 * The number of URLs crawled at this host.
	 * @return
	 */
	public abstract long getCrawledURLs();
	
	/**
	 * The number of URLs that were successfully fetched (and not only attempted)
	 * @return the no. of successfully fetched URLs
	 */
	public abstract long getSuccessfullyFetchedURLs();
	
	/**
	 * The average fetch duration observed at this host (in milliseconds).
	 * @return the average fetch duration
	 */
	public abstract double getAverageFetchDuration();
	
	
	/**
	 * The average number of retries over all (eventually) successful fetches.
	 * @return the average retry rate
	 */
	public abstract double getAverageRetryRate();
	
	/**
	 * The distribution of Heritrix fetch status codes for the URLs crawled at
	 * this host. The return value is a map that has the encountered fetch status
	 * codes (200, 404, -1, ...) as keys, and the number of URLs that have ended
	 * with that status as values. 
	 * @return the fetch status distribution
	 */
	public abstract Map<String, Integer> getFetchStatusDistribution();
	
	/**
	 * The distribution of MIME content types for the URLs crawled at this
	 * host. The return value is a map that has the mime type names as
	 * keys, and the number of URLs that have returned that MIME type
	 * as values.
	 * @return the content type distribution
	 */
	public abstract Map<String, Integer> getContentTypeDistribution();
	
	/**
	 * Returns the virus stats that have been recorded for this host. The
	 * return value is a map that has the name of the viruses as keys,
	 * and the number of URLs infected with that virus as values.
	 * @return the virus stats
	 */
	public abstract Map<String, Integer> getVirusStats();

	/**
	 * Returns the percentage of HTTP request that were precluded by Robots.txt rules.
	 * @return the percentage of blocks caused by robots.txt
	 */
	public abstract double getRobotsBlockPercentage();
	
	/**
	 * Returns the percentage of HTTP requests that received an HTTP 3xx response.
	 * @return the percentage of 3xx responses
	 */
	public abstract double getRedirectPercentage();
	
	/**
	 * Returns the ratio of text vs. non-text MIME types.
	 * @return the text vs. non-text content type ratio
	 */
	public abstract double getTextToNoneTextRatio();
	
	/**
	 * Helper method to split a host name into tokens. Host names
	 * will be split at the following characters: '.', '-', '_'
	 * 
	 * Note: keeping this in a separate method, although it's a
	 * one-liner. Possibly we want to do more elaborate things in the future.
	 * 
	 * @param hostname the host name
	 * @return the tokens
	 */
	public static String[] tokenizeName(String hostname) {
		return hostname.split("-|_|\\.");
	}
	
	@Override
	public String toString() {
		return getHostname() + " (last access: " + getLastAccess() + ", first access: " + getFirstAccess() + ", average delay: " + getAverageFetchDuration() + ")";
	}
	
}
