package uk.bl.monitrix.database.mongodb;

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
	public static final String COLLECTION_ALERT_LOG = "alert_log";
	public static final String COLLECTION_CRAWL_LOG = "crawl_log";
	public static final String COLLECTION_CRAWL_STATS = "crawl_stats";
	public static final String COLLECTION_KNOWN_HOSTS = "known_hosts";
	
	/** Database field keys (Alert Log collection) **/
	public static final String FIELD_ALERT_LOG_TIMESTAMP = "timestamp";
	public static final String FIELD_ALERT_LOG_OFFENDING_HOST = "offending_host";
	public static final String FIELD_ALERT_LOG_ALERT_TYPE = "alert_type";
	public static final String FIELD_ALERT_LOG_DESCRIPTION = "alert_description";

	/** Database field keys (Crawl Log collection) **/
	public static final String FIELD_CRAWL_LOG_TIMESTAMP = "timestamp";
	public static final String FIELD_CRAWL_LOG_LINE = "line";
	public static final String FIELD_CRAWL_LOG_HOST = "host";
	public static final String FIELD_CRAWL_LOG_SUBDOMAIN = "subdomain";
	public static final String FIELD_CRAWL_LOG_CRAWLER_ID = "crawler_id";
	public static final String FIELD_CRAWL_LOG_HTTP_CODE = "http_code";
	
	/** Database field keys (Crawl Stats collection) **/
	public static final String FIELD_CRAWL_STATS_TIMESTAMP = "timestamp";
	public static final String FIELD_CRAWL_STATS_DOWNLOAD_VOLUME = "download_volume";
	public static final String FIELD_CRAWL_STATS_NUMBER_OF_URLS_CRAWLED = "number_of_urls_crawled";
	public static final String FIELD_CRAWL_STATS_NEW_HOSTS_CRAWLED = "new_hosts_crawled";
	public static final String FIELD_CRAWL_STATS_COMPLETED_HOSTS = "completed_hosts";
	
	/** Database field keys (Known Hosts collection) **/
	public static final String FIELD_KNOWN_HOSTS_HOSTNAME = "host_name";
	public static final String FIELD_KNOWN_HOSTS_HOSTNAME_TOKENIZED = "host_name_tokenized";
	public static final String FIELD_KNOWN_HOSTS_SUBDOMAINS = "subdomains";
	public static final String FIELD_KNOWN_HOSTS_SUBDOMAINS_TOKENIZED = "subdomains_tokenized";
	public static final String FIELD_KNOWN_HOSTS_FIRST_ACCESS = "first_access";
	public static final String FIELD_KNOWN_HOSTS_LAST_ACCESS = "last_access";
	
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
