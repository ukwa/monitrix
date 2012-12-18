package uk.bl.monitrix.db.mongodb.preaggregatedstats;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.DBObject;

/**
 * A wrapper around DBObjects stored in the 'Pre-Aggregated Stats' collection.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class PreAggregatedStatsDBO implements Comparable<PreAggregatedStatsDBO> {
	
	DBObject dbo;
	
	public PreAggregatedStatsDBO(DBObject dbo) {
		this.dbo = dbo;
	}
	
	public long getTimeslot() {
		return (Long) dbo.get(MongoProperties.FIELD_PRE_AGGREGATED_TIMESLOT);
	}
	
	public void setTimeslot(long timeslot) {
		dbo.put(MongoProperties.FIELD_PRE_AGGREGATED_TIMESLOT, timeslot);
	}
	
	public int getDownloadVolume() {
		return (Integer) dbo.get(MongoProperties.FIELD_PRE_AGGREGATED_DOWNLOAD_VOLUME);
	}
	
	public void setDownloadVolume(int volume) {
		dbo.put(MongoProperties.FIELD_PRE_AGGREGATED_DOWNLOAD_VOLUME, volume);
	}
	
	public long getNumberOfURLs() {
		return (Long) dbo.get(MongoProperties.FIELD_PRE_AGGREGATED_NUMBER_OF_URLS);
	}
	
	public void setNumberOfURLs(long numberOfURLs) {
		dbo.put(MongoProperties.FIELD_PRE_AGGREGATED_NUMBER_OF_URLS, numberOfURLs);
	}
	
	public long getNumberOfNewHostsCrawled() {
		return (Long) dbo.get(MongoProperties.FIELD_PRE_AGGREGATED_NEW_HOSTS_CRAWLED);
	}
	
	public void setNumberOfNewHostsCrawled(long newHostsCrawled) {
		dbo.put(MongoProperties.FIELD_PRE_AGGREGATED_NEW_HOSTS_CRAWLED, newHostsCrawled);
	}

	@Override
	public int compareTo(PreAggregatedStatsDBO other) {
		return (int) (this.getTimeslot() - other.getTimeslot());
	}

}
