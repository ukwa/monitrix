package uk.bl.monitrix.model;

import java.util.List;

/**
 * An host search result.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class SearchResult {
	
	private String query;
	
	private long totalNumberOfResults;
	
	private List<SearchResultItem> results;
	
	private int limit;
	
	private long offset;
	
	private long took;
	
	public SearchResult(String query, long totalResults, List<SearchResultItem> resultPage, int limit, int offset, long took) {
		this.query = query;
		this.totalNumberOfResults = totalResults;
		this.results = resultPage;
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
		return totalNumberOfResults;
	}
	
	/**
	 * The list of host names in this search result page
	 * @return the host names
	 */
	public List<SearchResultItem> resultPage() {
		return results;
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
