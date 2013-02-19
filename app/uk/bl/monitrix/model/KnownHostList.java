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
	 * Searches the host list with the specified (e.g. keyword) query.
	 * Refer to documentation of specific implementations for the types of 
	 * queries supported!
	 * @param query the search query
	 * @param limit the max number of results to return
	 * @param offset the result page offset
	 * @return the search result
	 */
	public HostSearchResult searchHosts(String query, int limit, int offset);
	
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
	
	/**
	 * An host search result.
	 */
	public class HostSearchResult {
		
		private String query;
		
		private long totalNumberOfHosts;
		
		private List<String> resultPage;
		
		private int limit;
		
		private long offset;
		
		private long took;
		
		public HostSearchResult(String query, long totalResults, List<String> resultPage, int limit, int offset, long took) {
			this.query = query;
			this.totalNumberOfHosts = totalResults;
			this.resultPage = resultPage;
			this.limit = limit;
			this.offset = offset;
			this.took = took;
		}
		
		public String query() {
			return query;
		}
		
		/**
		 * The total number of hosts for this search
		 * @return the total number of hosts
		 */
		public long totalResults() {
			return totalNumberOfHosts;
		}
		
		/**
		 * The list of host names in this search result page
		 * @return the host names
		 */
		public List<String> resultPage() {
			return resultPage;
		}
		
		/**
		 * The current page limit
		 * @return the page limit
		 */
		public int limit() {
			return limit;
		}
		
		/**
		 * The current page offset
		 * @return the page offset
		 */
		public long offset() {
			return offset;
		}
		
		/**
		 * The time it took to process the search query
		 * @return the query duration
		 */
		public long took() {
			return took;
		}
		
	}
	
}
