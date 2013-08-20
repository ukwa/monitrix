package uk.bl.monitrix.database.cassandra;

import java.io.IOException;
import java.util.HashMap;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
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
	private static final long HOUR_AS_MILLIS = 1000*60*60;
	
	// Ingest schedule
	private IngestSchedule ingestSchedule;
	
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
			   System.out.printf("Connected to cluster: %s\n", 
			         metadata.getClusterName());
			   for ( Host host : metadata.getAllHosts() ) {
			      System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
			         host.getDatacenter(), host.getAddress(), host.getRack());
			   }
			   session = cluster.connect();
		
		this.ingestSchedule = new CassandraIngestSchedule(session);
		this.crawlLog = new CassandraCrawlLog(session);
		this.crawlStats = new CassandraCrawlStats(session);
		this.knownHosts = new CassandraKnownHostList(session);
		this.alertLog = new CassandraAlertLog(session);
		this.virusLog = new CassandraVirusLog(session);
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
