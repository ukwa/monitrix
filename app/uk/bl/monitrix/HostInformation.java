package uk.bl.monitrix;

import java.util.List;

/**
 * The Host Information interface. Provides access to analytics information 
 * concerning a particular host, as extracted from the Heritrix logs.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface HostInformation {
	
	/**
	 * Host name.
	 * @return the host name
	 */
	public String getHostname();
	
	/**
	 * UNIX timestamp of the first recorded access to this host.
	 * @return
	 */
	public long getFirstAccess();
	
	/**
	 * UNIX timestamp of the last recorded access to this host.
	 * @return the last access to the host
	 */
	public long getLastAccess();
	
	/**
	 * Number of URLs that were crawled at this host.
	 * @return the number of crawled URLs
	 */
	public long countCrawledURLs();
	
	/**
	 * List of crawlers that visited this host.
	 * @return the crawlers that crawled this host
	 */
	public List<String> getCrawlers();
	
	/**
	 * Distribution of HTTP/Heritrix return codes for this host.
	 * @return the distribution of HTTP/Heritrix return codes
	 */
	public List<PieChartValue> getHTTPCodes();

}
