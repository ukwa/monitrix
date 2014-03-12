package uk.bl.monitrix.database.cassandra;

import java.io.IOException;
import java.util.HashMap;

import play.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.database.ExtensionTable;
import uk.bl.monitrix.database.cassandra.model.CassandraAlertLog;
import uk.bl.monitrix.database.cassandra.model.CassandraCrawlLog;
import uk.bl.monitrix.database.cassandra.model.CassandraCrawlStats;
import uk.bl.monitrix.database.cassandra.model.CassandraIngestSchedule;
import uk.bl.monitrix.database.cassandra.model.CassandraKnownHostList;
import uk.bl.monitrix.database.cassandra.model.CassandraVirusLog;
import uk.bl.monitrix.model.AlertLog;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.CrawlStats;
import uk.bl.monitrix.model.IngestSchedule;
import uk.bl.monitrix.model.KnownHostList;
import uk.bl.monitrix.model.VirusLog;

/**
 * A Cassandra-backed implementation of {@link DBConnector}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraDBConnector implements DBConnector {
	
	// The Cassandra Cluster
	private Cluster cluster;

	// The Cassandra Session
	private Session session;
	
	// The resolution of the time-wise logging
	public static final long HOUR_AS_MILLIS = 1000*60*60;
	
	// Ingest schedule
	private volatile IngestSchedule ingestSchedule;
	
	// Crawl log
	private CrawlLog crawlLog;
	
	// Crawl stats
	private CrawlStats crawlStats;
	
	// Known hosts list
	private KnownHostList knownHosts;
	
	// Alert log
	private AlertLog alertLog;
	
	// Virus log
	private VirusLog virusLog;
	
	// Extension tables
	private HashMap<String, ExtensionTable> extensionTables = new HashMap<String, ExtensionTable>();
	
	public CassandraDBConnector() throws IOException {
		init(CassandraProperties.HOST, CassandraProperties.KEYSPACE, CassandraProperties.DB_PORT);
	}
	
	public CassandraDBConnector(String hostName, String keyspace, int dbPort) throws IOException {
		init(hostName, keyspace, dbPort);
	}
	
	private void init(String hostName, String keyspace, int dbPort) throws IOException {
		Logger.info("Initializing database connection");
		cluster = Cluster.builder()
				.addContactPoint(hostName).build();
		Metadata metadata = cluster.getMetadata();
	    Logger.info("Connected to Cassandra cluster: " + metadata.getClusterName());
	    
		for ( Host host : metadata.getAllHosts() ) {
		    Logger.info("Datacenter: " + host.getDatacenter() + "; Host: " + host.getAddress() + "; Rack: " + host.getRack());
		}
		session = cluster.connect();
		
		// Add schema if needed
		if (!schemaExists()) 
			createSchema();
		
		this.ingestSchedule = new CassandraIngestSchedule(session);
		this.crawlLog = new CassandraCrawlLog(session);
		this.crawlStats = new CassandraCrawlStats(session);
		this.knownHosts = new CassandraKnownHostList(session);
		this.alertLog = new CassandraAlertLog(session);
		this.virusLog = new CassandraVirusLog(session);
	}
	
	private boolean schemaExists() {
		Logger.info("Checking if schema exists...");
		ResultSet rows = session.execute("select * from system.schema_keyspaces;");
		for (Row r : rows ) {
			if (r.getString("keyspace_name").equals(CassandraProperties.KEYSPACE))
				return true;
		}
		Logger.info("No schema defined");
		return false;
	}
	
	private void createSchema() {
		Logger.info("Creating schema...");
		
		session.execute("CREATE KEYSPACE " + CassandraProperties.KEYSPACE + " WITH replication " + 
				"= {'class':'SimpleStrategy', 'replication_factor':1};");

		// This is a fairly denormalised model, with URL-based lookup for frontier management
		// and de-duplication, and time-wise lookups
		
		session.execute(
				"CREATE TABLE crawl_uris.log(" +
					"log_id varchar , " + 
					"log_ts varchar PRIMARY KEY , " +  
					"status_code varchar, " +
					"downloaded_bytes varchar, " + 
					"uri varchar, " +
					"discovery_path varchar, " + 
					"referer varchar, " +
					"content_type varchar, " + 
					"worker_thread varchar, " + 
					"fetch_ts varchar, " + 
					"hash varchar, " + 
					"annotations varchar, " + 
					"ip_address varchar, " + 
					"line varchar, " + 
					"coarse_ts varchar, " + 
					"long_log_ts varchar ) " + 
				"WITH COMPACT STORAGE;");
		
		session.execute(
				"CREATE TABLE crawl_uris.crawls(" + 
					"crawl_id varchar PRIMARY KEY, " +
					"start_ts varchar, " + 
					"end_ts varchar, " + 
					"ingested_lines varchar, " + 
					"revisit_records varchar ) " + 
				"WITH COMPACT STORAGE;");

		session.execute(
				"CREATE TABLE crawl_uris.crawl_stats(" + 
					"crawl_id varchar, " +
					"stat_ts bigint, " + 
					"downloaded_bytes bigint, " + 
					"uris_crawled bigint, " +
					"new_hosts bigint, " +
					"completed_hosts bigint, " +
					"PRIMARY KEY (crawl_id, stat_ts) );"); 
		
		session.execute(
				"CREATE TABLE crawl_uris.alert_log(" +
					"timestamp bigint, " +
					"crawl_id varchar, " +
					"offending_host varchar, " +
					"alert_type varchar, " +
					"alert_description varchar, " +
					"PRIMARY KEY (crawl_id, timestamp) );");
		
		session.execute(
				"CREATE TABLE crawl_uris.virus_log(" +
					"virus_name varchar PRIMARY KEY, " +
					"occurences bigint )" +
				"WITH COMPACT STORAGE;");
		
		// Create some indexes to help with the time-wise lookups and filtering.
		
		// Crawl log indexes
		session.execute("CREATE INDEX log_id on crawl_uris.log(log_id);");
		session.execute("CREATE INDEX coarse_ts on crawl_uris.log(coarse_ts);");
		session.execute("CREATE INDEX annotations on crawl_uris.log(annotations);");
		session.execute("CREATE INDEX uri on crawl_uris.log(uri);");
		session.execute("CREATE INDEX long_log_ts on crawl_uris.log(long_log_ts);");
		
		// Crawl stats indexes
		// session.execute("CREATE INDEX stat_ts on crawl_uris.crawl_stats(stat_ts)");

		/*
		session.execute("CREATE INDEX host_idx ON crawl_uris.log (host)");
		session.execute("CREATE INDEX domain_idx ON crawl_uris.log (domain)");
		session.execute("CREATE INDEX status_code_idx ON crawl_uris.log (status_code)");
		session.execute("CREATE INDEX log_id_idx ON crawl_uris.log (log_id)");
		*/
		
		/* Also allow pure hash-based lookups:
		session.execute(
		"CREATE TABLE crawl_uris.hashes (" +
				"hash text," +
				"fetch_ts timestamp," +
				"uri text," +
				"PRIMARY KEY (hash)" +
		");");
		
		// Crawl-level data.
		session.execute(
				"CREATE TABLE crawl_uris.crawls (" +
						"crawl_id text," +
						"start_ts timestamp," +
						"end_ts timestamp," +
						"profile text," +
						"PRIMARY KEY (crawl_id)" +
				");");
		session.execute("CREATE INDEX profile_idx ON crawl_uris.crawls (profile)");

		// Add a crawl log file table:
		session.execute(
		"CREATE TABLE crawl_uris.log_files (" +
				"path text," +
				"crawler_id text," +
				"is_monitored boolean," +
				"PRIMARY KEY (path)" +
		");");
		session.execute("CREATE INDEX crawler_id_idx ON crawl_uris.log_files (crawler_id)");
		session.execute(
		"CREATE TABLE crawl_uris.log_file_counters (" +
				"path text," +
				"ingested_lines counter," +
				"revisit_records counter," +
				"PRIMARY KEY (path)" +
		");");
		
		
		// Alerts log
		session.execute(
		"CREATE TABLE crawl_uris.alerts (" +
				"host text," +
				"alert_ts timestamp," +
				"alert_type text," +
				"description text," +
				"PRIMARY KEY (host, alert_ts)" +
		");");
		session.execute("CREATE INDEX alert_type_idx ON crawl_uris.alerts (alert_type)");
		
		// Annotated URLs
		// FIXME Probably not required. Any annotations should probably be stowed as alerts or stats. 
		// BUT changing this means API changes to strip annotations out of the higher-level API.
		session.execute(
		"CREATE TABLE crawl_uris.annotations (" +
				"annotation text," +
				"url text," +
				"log_ts timestamp," +
				"host text, " +
				"PRIMARY KEY (annotation, url, log_ts)" +
		");");
		session.execute("CREATE INDEX annotation_host_idx ON crawl_uris.annotations (host)");
		
		// Virus log:
		session.execute(
		"CREATE TABLE crawl_uris.virus_log (" +
				"virus_name text," +
				"occurences map<text,int>," +
				"PRIMARY KEY (virus_name)" +
		");");
		//session.execute("CREATE INDEX alert_type_idx ON crawl_uris.alerts (alert_type)");
		
		// Crawl stats:
		session.execute(
		"CREATE TABLE crawl_uris.stats (" +
				"crawl_id text," +
				"stat_ts timestamp," +
				"downloaded_bytes counter," +
				"uris_crawled counter," +
				"new_hosts counter," +
				"completed_hosts counter," +
				"PRIMARY KEY (crawl_id, stat_ts)" +
		");");
		//session.execute("CREATE INDEX stats_crawl_id_idx ON crawl_uris.stats (crawl_id)");
		
		// FIXME Add histogram table for e.g. url compressibility, by crawl? Actually, need URL.
		
		// Known hosts lookup:
		session.execute(
		"CREATE TABLE crawl_uris.known_hosts (" +
				"host text," +
				"first_access timestamp," +
				"last_access timestamp," +
				"tld text," +
				"domain text," +
				"subdomain text," +
				"successfully_fetched_urls bigint," +
				"fetch_status_codes map<text,int>," +
				"crawled_urls bigint," +
				"avg_fetch_duration double," +
				"avg_retry_rate double," +
				"content_types map<text,int>," +
				"crawlers list<text>," +
				"text_to_nontext_ratio double," +
				"redirect_percentage double," +
				"robots_block_percentage double," +
				"virus_stats map<text,int>," +
				"PRIMARY KEY (host)" +
		");");
		session.execute("CREATE INDEX tld_idx ON crawl_uris.known_hosts (tld)");
		session.execute("CREATE INDEX tld_domain_idx ON crawl_uris.known_hosts (domain)");
		session.execute("CREATE INDEX avg_retry_rate_idx ON crawl_uris.known_hosts (avg_retry_rate)");
		
		// FIXME Add counter columns for each host
		session.execute(
		"CREATE TABLE crawl_uris.known_host_counters (" +
				"host text," +
				"uris counter," +
				"retries counter," +
				"duration counter," +
				"successfully_fetched_uris counter," +
				"redirects counter," +
				"blocked_by_robots counter," +
				"text_resources counter," +
				"text_run counter," +
				"subdomains counter," +
				"PRIMARY KEY (host)" +
		");");
		
		// Add histogram column.
		// (crawl,histogram_type, range), counter.
		// i.e., for each crawl, look up each histogram, sorted by range (x-axis lower bucket), giving totals.
		// BUT remember, need same ranges with same quantisation as indexes on the corresponding tables
		// (e.g. known_hosts for percentages and counts on hosts, log for compressibility of uris)
		session.execute(
		"CREATE TABLE crawl_uris.known_host_histograms (" +
				"host text," +
				"histogram_type text," +
				"range double," +
				"total counter," +
				"PRIMARY KEY (host,histogram_type,range)" +
		");");
		
		// Known tlds:
		session.execute(
		"CREATE TABLE crawl_uris.known_tlds (" +
				"tld text," +
				"crawled_urls counter," +
				"PRIMARY KEY (tld)" +
				");");
		*/
	}
	
	public void dropSchema() {
		if (schemaExists() )
			session.execute("DROP KEYSPACE " + CassandraProperties.KEYSPACE);
	}
	
	public Session getSession() {
		return this.session;
	}
	
	
	@Override
	public IngestSchedule getIngestSchedule() {
		return ingestSchedule;
	}

	@Override
	public CrawlLog getCrawlLog() {
		return crawlLog;
	}

	@Override
	public CrawlStats getCrawlStats() {
		return crawlStats;
	}

	@Override
	public AlertLog getAlertLog() {
		return alertLog;
	}
	
	@Override
	public VirusLog getVirusLog() {
		return virusLog;
	}

	@Override
	public KnownHostList getKnownHostList() {
		return knownHosts;
	}

	@Override
	public void close() {
		this.session.shutdown();
		this.cluster.shutdown();
	}

	
	@Override
	@SuppressWarnings("unchecked")
	public <T extends ExtensionTable> T getExtensionTable(String name, Class<T> type) {
		ExtensionTable ext = extensionTables.get(name);
		
		if (ext == null) {
			try {
				ext = null;//type.getConstructor(DB.class).newInstance(db);
				extensionTables.put(name, ext);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
			
		return (T) ext;
	}

}
