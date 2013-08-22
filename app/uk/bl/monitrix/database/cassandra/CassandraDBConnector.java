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
		cluster = Cluster.builder()
				.addContactPoint(hostName).build();
		Metadata metadata = cluster.getMetadata();
	    Logger.info("Connected to Cassandra cluster: " + metadata.getClusterName());
		for ( Host host : metadata.getAllHosts() ) {
		    Logger.info("Datacenter: "+host.getDatacenter()+"; Host: "+host.getAddress()+"; Rack: "+host.getRack());
		}
		session = cluster.connect();
		
		// Add schema if needed:
		this.dropSchema();
		if( ! this.isSchemaThere() ) this.createSchema();
		
		this.ingestSchedule = new CassandraIngestSchedule(session);
		this.crawlLog = new CassandraCrawlLog(session);
		this.crawlStats = new CassandraCrawlStats(session);
		this.knownHosts = new CassandraKnownHostList(session);
		this.alertLog = new CassandraAlertLog(session);
		this.virusLog = new CassandraVirusLog(session);
	}
	
	private boolean isSchemaThere() {
		ResultSet rows = session.execute("select * from system.schema_keyspaces;");
		for( Row r : rows ) {
			if( "crawl_uris".equals(r.getString("keyspace_name"))) return true;
		}
		return false;
	}
	
	private void createSchema() {
		session.execute("CREATE KEYSPACE crawl_uris WITH replication " + 
				"= {'class':'SimpleStrategy', 'replication_factor':1};");

		// This is a fairly denormalised model, with URL-based lookup for frontier management
		// and de-duplication, and time-wise lookups 
		session.execute(
				"CREATE TABLE crawl_uris.uris (" +
						"uri text," +
						"log_ts timestamp," +
						"coarse_ts timestamp," +
						"fetch_ts timestamp," +
						"status_code int," +
						"hash text," +
						"PRIMARY KEY (uri, log_ts)" +
				");");
		
		// Could manage wide-rows by hand, but not much point now CQL3 handles composite keys:
		// http://www.datastax.com/docs/1.1/ddl/column_family		
//		session.execute(
//				"CREATE TABLE crawl_uris.log_index (" +
//						"coarse_ts timestamp," +
//						"PRIMARY KEY (coarse_ts)" +
//				");");
		
		// Composite keys and clustering allow us to do this instead:
		session.execute(
				"CREATE TABLE crawl_uris.log (" +
						"coarse_ts timestamp," +
						"log_ts timestamp," +
						"uri text," +
						"fetch_ts timestamp," +
						"host text," +
						"domain text, " + 
						"subdomain text, " + 
						"status_code int," +
						"hash text," +
						"log_id text," +
						"annotations text," +
						"discovery_path text," +
						"compressibility double," +
						"content_type text," +
						"download_size bigint," +
						"fetch_duration int," +
						"referer text," +
						"retries int," +
						"worker_thread text," +
						"PRIMARY KEY (coarse_ts, log_ts)" +
				");");
		
		// Create some indexes to help with the time-wise lookups and filtering.
		session.execute("CREATE INDEX host_idx ON crawl_uris.log (host)");
		session.execute("CREATE INDEX domain_idx ON crawl_uris.log (domain)");
		session.execute("CREATE INDEX status_code_idx ON crawl_uris.log (status_code)");
		session.execute("CREATE INDEX log_id_idx ON crawl_uris.log (log_id)");

		// Also allow pure hash-based lookups:
		session.execute(
		"CREATE TABLE crawl_uris.hashes (" +
				"hash text," +
				"fetch_ts timestamp," +
				"uri text," +
				"PRIMARY KEY (hash)" +
		");");
		
		// Crawl-level data.
		// Processing adds dynamically named columns:
		//   status_code:200 
		// (perhaps use counters?)
		session.execute(
				"CREATE TABLE crawl_uris.crawls (" +
						"crawl_id text," +
						"start_ts timestamp," +
						"end_ts timestamp," +
						"profile text," +
						"total_urls bigint," +
						"PRIMARY KEY (crawl_id, start_ts)" +
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
				"PRIMARY KEY (path)" +
		");");
		
		
		// Alerts log
		// host, alert_ts, alert_type, description
		session.execute(
		"CREATE TABLE crawl_uris.alerts (" +
				"host text," +
				"alert_ts timestamp," +
				"alert_type text," +
				"description text," +
				"PRIMARY KEY (host, alert_ts)" +
		");");
		session.execute("CREATE INDEX alert_type_idx ON crawl_uris.alerts (alert_type)");
		
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
				"stat_ts timestamp," +
				"crawl_id text," +
				"stats_map map<text,int>," +
				"PRIMARY KEY (stat_ts, crawl_id)" +
		");");
		//session.execute("CREATE INDEX stats_crawl_id_idx ON crawl_uris.stats (crawl_id)");
		
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
		
	}
	
	public void dropSchema() {
		session.execute("DROP KEYSPACE crawl_uris");
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
