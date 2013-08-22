package uk.bl.monitrix.database.cassandra;

import play.Configuration;
import play.Logger;
import play.Play;

/**
 * Central access point to Mongo config options, DB table and field names, etc.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraProperties {
	
	private static Configuration config = Play.application().configuration();
	
	/** Database settings, as configured in application.conf **/
	public static final String HOST = config.getString("cassandra.host");
	public static int DB_PORT; 
	public static final String KEYSPACE = config.getString("cassandra.keyspace");
	
	/** Database collection names **/
	public static final String COLLECTION_INGEST_SCHEDULE = "ingest_schedule";
	public static final String COLLECTION_ALERT_LOG = "alert_log";
	public static final String COLLECTION_CRAWL_LOG = "crawl_log";
	public static final String COLLECTION_CRAWL_STATS = "crawl_stats";
	public static final String COLLECTION_KNOWN_HOSTS = "known_hosts";
	public static final String COLLECTION_VIRUS_LOG = "virus_log";
	
	/** Database field keys (Ingest Schedule collection) **/
	public static final String FIELD_INGEST_SCHEDULE_ID = "_id";
	public static final String FIELD_INGEST_SCHEDULE_PATH = "path";
	public static final String FIELD_INGEST_SCHEDULE_CRAWLER_ID = "crawler_id";
	public static final String FIELD_INGEST_SCHEDULE_LINES = "ingested_lines";
	public static final String FIELD_INGEST_SCHEDULE_MONITORED = "is_monitored";
	
	/** Database field keys (Alert Log collection) **/
	public static final String FIELD_ALERT_LOG_TIMESTAMP = "timestamp";
	public static final String FIELD_ALERT_LOG_OFFENDING_HOST = "offending_host";
	public static final String FIELD_ALERT_LOG_ALERT_TYPE = "alert_type";
	public static final String FIELD_ALERT_LOG_DESCRIPTION = "alert_description";

	/** Database field keys (Crawl Log collection) **/
	public static final String FIELD_CRAWL_LOG_LOG_ID = "log_id";
	public static final String FIELD_CRAWL_LOG_TIMESTAMP = "log_ts";
	public static final String FIELD_CRAWL_LOG_URL = "uri";
	public static final String FIELD_CRAWL_LOG_HOST = "host";
	public static final String FIELD_CRAWL_LOG_DOMAIN = "domain";
	public static final String FIELD_CRAWL_LOG_SUBDOMAIN = "subdomain";
	public static final String FIELD_CRAWL_LOG_CRAWLER_ID = "worker_thread";
	public static final String FIELD_CRAWL_LOG_HTTP_CODE = "status_code";
	public static final String FIELD_CRAWL_LOG_ANNOTATIONS = "annotations";
	public static final String FIELD_CRAWL_LOG_ANNOTATIONS_TOKENIZED = "annotations_tokenized";
	public static final String FIELD_CRAWL_LOG_RETRIES = "retries";
	public static final String FIELD_CRAWL_LOG_COMPRESSABILITY = "compressability";
	public static final String FIELD_CRAWL_LOG_LINE = "line";
	
	/** Database field keys (Crawl Stats collection) **/
	public static final String FIELD_CRAWL_STATS_TIMESTAMP = "timestamp";
	public static final String FIELD_CRAWL_STATS_DOWNLOAD_VOLUME = "download_volume";
	public static final String FIELD_CRAWL_STATS_NUMBER_OF_URLS_CRAWLED = "number_of_urls_crawled";
	public static final String FIELD_CRAWL_STATS_NEW_HOSTS_CRAWLED = "new_hosts_crawled";
	public static final String FIELD_CRAWL_STATS_COMPLETED_HOSTS = "completed_hosts";
	
	/** Database field keys (Known Hosts collection) **/
	public static final String FIELD_KNOWN_HOSTS_HOSTNAME = "host";
	public static final String FIELD_KNOWN_HOSTS_TLD = "tld";
	public static final String FIELD_KNOWN_HOSTS_DOMAIN = "domain";
	public static final String FIELD_KNOWN_HOSTS_SUBDOMAIN = "subdomain";
	public static final String FIELD_KNOWN_HOSTS_FIRST_ACCESS = "first_access";
	public static final String FIELD_KNOWN_HOSTS_LAST_ACCESS = "last_access";
	public static final String FIELD_KNOWN_HOSTS_CRAWLERS = "crawlers";
	public static final String FIELD_KNOWN_HOSTS_CRAWLED_URLS = "crawled_urls";
	public static final String FIELD_KNOWN_HOSTS_SUCCESSFULLY_FETCHED_URLS = "successfully_fetched_urls";
	public static final String FIELD_KNOWN_HOSTS_AVG_FETCH_DURATION = "avg_fetch_duration";
	public static final String FIELD_KNOWN_HOSTS_AVG_RETRY_RATE = "avg_retry_rate";	
	public static final String FIELD_KNOWN_HOSTS_FETCH_STATUS_CODES = "fetch_status_codes";
	public static final String FIELD_KNOWN_HOSTS_CONTENT_TYPES = "content_types";
	public static final String FIELD_KNOWN_HOSTS_VIRUS_STATS = "virus_stats";
	public static final String FIELD_KNOWN_HOSTS_REDIRECT_PERCENTAGE = "redirect_percentage";
	public static final String FIELD_KNOWN_HOSTS_ROBOTS_BLOCK_PERCENTAGE = "robots_block_percentage";
	public static final String FIELD_KNOWN_HOSTS_TEXT_TO_NONTEXT_RATIO = "text_to_nontext_ratio";
	
	/** Database field keys (Virus Log collection **/
	public static final String FIELD_VIRUS_LOG_NAME = "virus_name";
	public static final String FIELD_VIRUS_LOG_OCCURENCES = "occurences";
	
	/** Bulk insert chunk size **/
	public static final int BULK_INSERT_CHUNK_SIZE = 500000;
	
	/** Resolution of the data pre-aggregation raster (in milliseconds) **/ 
	public static final int PRE_AGGREGATION_RESOLUTION_MILLIS = 60000;
	
	static {
		try {
			DB_PORT = Integer.parseInt(config.getString("cassandra.port"));
		} catch (Throwable t) {
			DB_PORT = 9160;
			Logger.warn("Error reading cassandra.port from application.conf - defaulting to "+DB_PORT);
		}
	}

}
