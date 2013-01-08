package uk.bl.monitrix.db.mongodb.model;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.DBObject;

/**
 * Wraps the DBObjects stored in the 'Pre-Aggregated Stats' collection.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class PreAggregatedStatsDBO implements Comparable<PreAggregatedStatsDBO> {
	
	DBObject dbo;
	
	public PreAggregatedStatsDBO(DBObject dbo) {
		this.dbo = dbo;
	}
	
	/**
	 * UNIX timestamp of the start time of this timeslot.
	 * @return the timeslot start time timestamp 
	 */
	public long getTimeslot() {
		return (Long) dbo.get(MongoProperties.FIELD_PRE_AGGREGATED_TIMESLOT);
	}
	
	/**
	 * Sets the UNIX timestamp of this timeslot's start time.
	 * @param timeslot the timeslot start time
	 */
	public void setTimeslot(long timeslot) {
		dbo.put(MongoProperties.FIELD_PRE_AGGREGATED_TIMESLOT, timeslot);
	}
	
	/**
	 * Accumulated data volume downloaded within this timeslot.
	 * @return the download volume in this timeslot
	 */
	public int getDownloadVolume() {
		return (Integer) dbo.get(MongoProperties.FIELD_PRE_AGGREGATED_DOWNLOAD_VOLUME);
	}
	
	/**
	 * Sets the accumulated data volume downloaded within this timeslot.
	 * @param volume the download volume in this timeslot
	 */
	public void setDownloadVolume(int volume) {
		dbo.put(MongoProperties.FIELD_PRE_AGGREGATED_DOWNLOAD_VOLUME, volume);
	}
	
	/**
	 * Number of URLs crawled within this timeslot.
	 * @return the number of URLs crawled in this timeslot
	 */
	public long getNumberOfURLs() {
		return (Long) dbo.get(MongoProperties.FIELD_PRE_AGGREGATED_NUMBER_OF_URLS);
	}
	
	/**
	 * Sets the number of URLs crawled within this timeslot.
	 * @param numberOfURLs the number of URLs crawled in this timeslot
	 */
	public void setNumberOfURLs(long numberOfURLs) {
		dbo.put(MongoProperties.FIELD_PRE_AGGREGATED_NUMBER_OF_URLS, numberOfURLs);
	}
	
	/**
	 * Accumulated number of hosts that were visited for the first time in this crawl, 
	 * during this timeslot. (I.e. number of 'new host is crawled for the first time' events
	 * that happened in this timeslot.)
	 * @return the number of newly crawled hosts in this timeslot 
	 */
	public long getNumberOfNewHostsCrawled() {
		return (Long) dbo.get(MongoProperties.FIELD_PRE_AGGREGATED_NEW_HOSTS_CRAWLED);
	}
	
	/**
	 * Sets the accumulated number of hosts that were visited for the first time in
	 * this crawl, during this timeslot.
	 * @param newHostsCrawled the number of newly crawled hosts in this timeslot
	 */
	public void setNumberOfNewHostsCrawled(long newHostsCrawled) {
		dbo.put(MongoProperties.FIELD_PRE_AGGREGATED_NEW_HOSTS_CRAWLED, newHostsCrawled);
	}

	@Override
	public int compareTo(PreAggregatedStatsDBO other) {
		return (int) (this.getTimeslot() - other.getTimeslot());
	}

}
