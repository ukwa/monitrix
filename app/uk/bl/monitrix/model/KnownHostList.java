package uk.bl.monitrix.model;

import java.util.List;

/**
 * The known host list interface. Provides read/query access to the list of
 * crawled hosts.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface KnownHostList {
	
	/**
	 * Checks if the specified hostname is already in the known hosts list.
	 * @param hostname the hostname
	 * @return <code>true</code> if the host is already in the list
	 */
	public boolean isKnown(String hostname);

	/**
	 * Retrieves the host information for a specific host from the list.
	 * @param hostname the hostname
	 * @return the known host record
	 */
	public KnownHost getKnownHost(String hostname);
	
	/**
	 * Searches the list for a particular host, e.g. via keyword search.
	 * Refer to documentation of specific implementations for the types of 
	 * queries supported!
	 * @param query the search query
	 * @return the list of hostnames matching the query 
	 */
	public List<String> searchHost(String query);
	
	/**
	 * Retruns the names of the hosts which have been crawled since the
	 * specified timestamp.
	 * @param since the timestamp
	 * @return the list of hosts visited since the timestamp
	 */
	public List<KnownHost> getCrawledHosts(long since);
	
	/**
	 * Counts the number of hosts registered under a specific top-level domain.
	 * @param tld the top level domain
	 * @return
	 */
	public long countForTopLevelDomain(String tld);
	
	/**
	 * Returns the top-level domains encountered during the crawl.
	 * @return the list of top-level domains
	 */
	public List<String> getTopLevelDomains();
	
}
