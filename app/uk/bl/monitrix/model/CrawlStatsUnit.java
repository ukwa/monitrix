package uk.bl.monitrix.model;

import java.util.Set;

/**
 * The CrawlStatsUnit domain object interface. Encapsulates stats information that was pre-aggregated 
 * for a single base resolution timeslot.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public abstract class CrawlStatsUnit implements Comparable<CrawlStatsUnit> {
	
	/**
	 * UNIX timestamp of the start time of this unit timeslot.
	 * @return the timeslot start time 
	 */
	public abstract long getTimestamp(); 
	
	/**
	 * Accumulated data volume downloaded within this unit timeslot.
	 * @return the download volume in this timeslot
	 */
	public abstract int getDownloadVolume();
	
	/**
	 * Number of URLs crawled within this unit timeslot.
	 * @return the number of URLs crawled in this timeslot
	 */
	public abstract long getNumberOfURLsCrawled();
		
	/**
	 * Accumulated number of hosts that were visited for the first time in this crawl, 
	 * during this unit timeslot. (I.e. number of 'new host is crawled for the first
	 * time' events that happened in this timeslot.)
	 * @return the number of newly crawled hosts in this timeslot 
	 */
	public abstract long getNumberOfNewHostsCrawled();
	
	/**
	 * The number of hosts that were 'completed' (i.e. last accessed) within this unit timeslot.
	 * @return the number of completed hosts
	 */	
	public abstract long countCompletedHosts();
	
	/**
	 * The list of hostnames that were 'completed' (i.e. last accessed) within this unit timeslot.
	 * @return the list of completed hosts
	 */
	public abstract Set<String> getCompletedHosts();
	
	@Override
	public int compareTo(CrawlStatsUnit other) {
		return (int) (this.getTimestamp() - other.getTimestamp());
	}

}
