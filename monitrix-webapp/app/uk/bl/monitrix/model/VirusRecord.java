package uk.bl.monitrix.model;

import java.util.Map;

/**
 * The Virus Record domain object interface. Encapsulates all information collected about a
 * specific virus encountered during the crawl. 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface VirusRecord {
	
	/**
	 * The name of the virus.
	 * @return the name
	 */
	public String getName();
	
	/**
	 * Returns information about where the virus has been encountered. The
	 * return value is a map that has the name of the infected hosts as keys,
	 * and the number of URLs infected at that host as values.
	 * @return the virus occurences
	 */
	public Map<String, Integer> getOccurences();

}
