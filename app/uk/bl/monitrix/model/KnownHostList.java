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
	
}
