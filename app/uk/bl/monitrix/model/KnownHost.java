package uk.bl.monitrix.model;

/**
 * The Known Host domain object interface. Encapsulates the information collected about a specific host
 * during the crawl.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 *
 */
public abstract class KnownHost {
	
	/**
	 * Host name.
	 * @return the host name
	 */
	public abstract String getHostname();
	
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
	
}
