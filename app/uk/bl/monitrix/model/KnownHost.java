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
		return getHostname() + " (last access: " + getLastAccess() + ", first access: " + getFirstAccess() + ")";
	}
	
}
