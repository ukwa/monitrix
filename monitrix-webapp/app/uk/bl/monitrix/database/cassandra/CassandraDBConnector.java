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
		this.crawlStats = new CassandraCrawlStats(session, this.ingestSchedule);
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
				"CREATE TABLE crawl_uris.crawl_log(" +
					"log_id varchar , " + 
					"timestamp varchar, " +  
					"long_timestamp bigint, " +
					"coarse_timestamp bigint, " +
					"status_code int, " +
					"downloaded_bytes bigint, " + 
					"uri varchar, " +
					"host varchar, " +
					"domain varchar, " +
					"subdomain varchar, " +
					"discovery_path varchar, " + 
					"referer varchar, " +
					"content_type varchar, " + 
					"worker_thread varchar, " + 
					"fetch_ts bigint, " + 
					"hash varchar, " + 
					"annotations varchar, " + 
					"ip_address varchar, " + 
					"line varchar, " + 
					"PRIMARY KEY (hash, timestamp) ); ");
		
		session.execute(
				"CREATE TABLE crawl_uris.ingest_schedule(" + 
					"crawl_id varchar PRIMARY KEY, " +
					"log_path varchar, " + 
					"start_ts bigint, " + 
					"end_ts bigint, " + 
					"ingested_lines bigint, " + 
					"revisit_records bigint, "+
					"is_monitored boolean) " + 
				"WITH COMPACT STORAGE;");
		
		session.execute(
				"CREATE TABLE crawl_uris.known_hosts(" +
					"host varchar PRIMARY KEY, " +
					"tld varchar, " +
					"domain varchar, " +
					"subdomain varchar, " +
					"first_access bigint, " +
					"last_access bigint, " +
					"crawlers varchar, " +
					"crawled_urls bigint, " +
					"successfully_fetched_urls bigint, " +
					"avg_fetch_duration double, " +
					"avg_retry_rate double, " +	
					"fetch_status_codes varchar, " +
					"content_types varchar, " +
					"virus_stats varchar, " +
					"redirect_percentage double, " + 
					"robots_block_percentage double, " +
					"text_to_nontext_ratio double);");

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
		
		// Crawl log indexes
		session.execute("CREATE INDEX log_id on crawl_uris.crawl_log(log_id);");
		session.execute("CREATE INDEX long_log_ts on crawl_uris.crawl_log(long_timestamp);");
		session.execute("CREATE INDEX coarse_ts on crawl_uris.crawl_log(coarse_timestamp);");
		session.execute("CREATE INDEX uri on crawl_uris.crawl_log(uri);");
		session.execute("CREATE INDEX host on crawl_uris.crawl_log(host);");
		session.execute("CREATE INDEX annotations on crawl_uris.crawl_log(annotations);");
		
		// Ingest schedule indexes
		session.execute("CREATE INDEX log_path on crawl_uris.ingest_schedule(log_path);");
		
		// Known host table indexes
		session.execute("CREATE INDEX avg_fetch_duration on crawl_uris.known_hosts(avg_fetch_duration);");
		session.execute("CREATE INDEX avg_retry_rate on crawl_uris.known_hosts(avg_retry_rate);");
		session.execute("CREATE INDEX robots_block_percentage on crawl_uris.known_hosts(robots_block_percentage);");
		session.execute("CREATE INDEX redirect_percentage on crawl_uris.known_hosts(redirect_percentage);");
		
		// Alert log indexes
		session.execute("CREATE INDEX offending_host on crawl_uris.alert_log(offending_host);");
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
