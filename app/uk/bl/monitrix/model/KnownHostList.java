package uk.bl.monitrix.model;

import java.util.List;

/**
 * The known host list interface. Provides read/query access to the list of
 * crawled hosts.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface KnownHostList {
	
	/**
	 * Returns the total number of known hosts.
	 * @return the number of hosts
	 */
	public long count();
	
	/**
	 * Returns the number of host that had at least one URL successfully resolved.
	 * @return the number of successfully crawled hosts.
	 */
	public long countSuccessful();
	
	/**
	 * Returns the maximum average delay that was encountered over all hosts.
	 * @return the maximum average delay
	 */
	public long getMaxFetchDuration();
	
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
	 * Searches the host list with the specified (e.g. keyword) query.
	 * Refer to documentation of specific implementations for the types of 
	 * queries supported!
	 * @param query the search query
	 * @param limit the max number of results to return
	 * @param offset the result page offset
	 * @return the search result
	 */
	public SearchResult searchHosts(String query, int limit, int offset);
	
	/**
	 * Returns the hosts registered under a specific top-level domain, with pagination. 
	 * @param tld the top-level domain
	 * @param limit the pagination limit
	 * @param offset the pagination offset
	 * @return the search result
	 */
	public SearchResult searchByTopLevelDomain(String tld, int limit, int offset);

	/**
	 * Returns the hosts within a specific average delay bracket.
	 * @param min the minimum average delay
	 * @param max the maximum average delay
	 * @param limit the pagination limit
	 * @param offset the pagination offset
	 * @return the search result
	 */
	public SearchResult searchByAverageFetchDuration(long min, long max, int limit, int offset);
	
	/**
	 * Returns the hosts within a specified average retry rate bracket.
	 * @param min the minimum number of retries
	 * @param max the maximum number of retries
	 * @param limit the pagination limit
	 * @param offset the pagination offset
	 * @return the search result
	 */
	public SearchResult searchByAverageRetries(int min, int max, int limit, int offset);
	
	/**
	 * Retruns the names of the hosts which have been crawled since the
	 * specified timestamp.
	 * @param since the timestamp
	 * @return the list of hosts visited since the timestamp
	 */
	public List<KnownHost> getCrawledHosts(long since);
	
	/**
	 * Returns the top-level domains encountered during the crawl.
	 * @return the list of top-level domains
	 */
	public List<String> getTopLevelDomains();
	
	/**
	 * Counts the number of hosts registered under a specific top-level domain.
	 * @param tld the top level domain
	 * @return
	 */
	public long countForTopLevelDomain(String tld);
	
}
