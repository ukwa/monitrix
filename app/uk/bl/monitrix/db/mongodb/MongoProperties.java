package uk.bl.monitrix.db.mongodb;

import play.Configuration;
import play.Logger;
import play.Play;

/**
 * Central access point to Mongo config options, DB table and field names, etc.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoProperties {
	
	private static Configuration config = Play.application().configuration();
	
	/** Database settings, as configured in application.conf **/
	public static final String DB_HOST = config.getString("mongo.host");
	public static int DB_PORT; 
	public static final String DB_NAME = config.getString("mongo.db.name");
	
	/** Database collection names **/
	public static final String COLLECTION_GLOBAL_STATS = "global_stats";
	public static final String COLLECTION_HERETRIX_LOG = "heretrix_log";
	public static final String COLLECTION_PRE_AGGREGATED_STATS = "pre_aggregated_stats";
	
	/** Database field keys (Global Stats collection) **/
	public static final String FIELD_GLOBAL_LINES_TOTAL = "lines_total";
	public static final String FIELD_GLOBAL_CRAWL_START = "crawl_started";
	public static final String FIELD_GLOBAL_CRAWL_LAST_ACTIVITIY = "crawl_last_activity";

	/** Database field keys (Heretrix Log collection) **/
	public static final String FIELD_LOG_TIMESTAMP = "timestamp";
	public static final String FIELD_LOG_LINE = "line";
	
	/** Database field keys (Pre-Aggregated Stats collection) **/
	public static final String FIELD_PRE_AGGREGATED_TIMESLOT = "timeslot";
	public static final String FIELD_PRE_AGGREGATED_DOWNLOAD_VOLUME = "download_volume";
	public static final String FIELD_PRE_AGGREGATED_NUMBER_OF_URLS = "number_of_urls";
	
	/** Bulk insert chunk size **/
	public static final int BULK_INSERT_CHUNK_SIZE = 500000;
	
	/** Resolution of the data pre-aggregation raster (in milliseconds) **/ 
	public static final int PRE_AGGREGATION_RESOLUTION_MILLIS = 60000;
	
	static {
		try {
			DB_PORT = Integer.parseInt(config.getString("mongo.port"));
		} catch (Throwable t) {
			Logger.warn("Error reading mongo.port from application.conf - defaulting to 27017");
			DB_PORT = 27017;
		}
	}

}
