/**
 * 
 */
package uk.bl.monitrix.database.cassandra.ingest;

import java.io.FileNotFoundException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import play.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import uk.bl.monitrix.heritrix.LogFileEntry;
import uk.bl.monitrix.heritrix.SimpleLogfileReader;
import uk.bl.monitrix.model.Alert;
import uk.bl.monitrix.model.Alert.AlertType;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class CassandraIngestTool {
	
	private Cluster cluster;
	
	private Session session;
	
	private static final long HOUR_AS_MILLIS = 1000*60*60;
	
	public void connect(String node) {
		   cluster = Cluster.builder()
		         .addContactPoint(node).build();
		   Metadata metadata = cluster.getMetadata();
		   System.out.printf("Connected to cluster: %s\n", 
		         metadata.getClusterName());
		   for ( Host host : metadata.getAllHosts() ) {
		      System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
		         host.getDatacenter(), host.getAddress(), host.getRack());
		   }
		   session = cluster.connect();
	}
	
	public boolean isSchemaThere() {
		ResultSet rows = session.execute("select * from system.schema_keyspaces;");
		for( Row r : rows ) {
			if( "crawl_uris".equals(r.getString("keyspace_name"))) return true;
		}
		return false;
	}
	
	public void createSchema() {
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
						"crawl_id text," +
						"annotations text," +
						"discovery_path text," +
						"compressibility double," +
						"content_type text," +
						"download_size bigint," +
						"fetch_duration int," +
						"referrer text," +
						"retries int," +
						"worker_thread text," +
						"PRIMARY KEY (coarse_ts, log_ts)" +
				");");
		
		// Create some indexes to help with the time-wise lookups and filtering.
		session.execute("CREATE INDEX host_idx ON crawl_uris.log (host)");
		session.execute("CREATE INDEX domain_idx ON crawl_uris.log (domain)");
		session.execute("CREATE INDEX status_code_idx ON crawl_uris.log (status_code)");
		session.execute("CREATE INDEX crawl_id_idx ON crawl_uris.log (crawl_id)");

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
						"total_urls counter," +
						"PRIMARY KEY (crawl_id, start_ts)" +
				");");

	}

	public void loadData() throws FileNotFoundException {
		PreparedStatement statement = getSession().prepare(
			      "INSERT INTO crawl_uris.uris " +
			      "(uri, coarse_ts, log_ts, fetch_ts, status_code, hash) " +
			      "VALUES (?, ?, ?, ?, ?, ?);");

		PreparedStatement log_statement = getSession().prepare(
			      "INSERT INTO crawl_uris.log " +
			      "(coarse_ts, log_ts, uri, fetch_ts, host, domain, subdomain, status_code, hash, crawl_id, " + 
			      "annotations, discovery_path, compressibility, content_type, download_size, " + 
			      "fetch_duration, referrer, retries, worker_thread) " +
			      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");


		SimpleLogfileReader slfr = new SimpleLogfileReader("/Users/andy/Documents/workspace/bl-crawler-tests/heritrix-3.1.2-SNAPSHOT/jobs/bl-test-crawl/heritrix/output/logs/bl-test-crawl/crawl.log.cp00001-20130605082749");
		Iterator<LogFileEntry> lines = slfr.iterator();
		while( lines.hasNext() ) {
			LogFileEntry l = lines.next();
			// SURT surt = SURT.fromURI();
			// AND canonicalised.
			// Skip if parse failed.
			if( l.getParseFailed() ){
				Logger.error("Could not parse "+l);
				continue;
			}
	
			// Check timestamp - should be the discovery/queue timestamp:
			Date log_ts = l.getLogTimestamp();
			Date fetch_ts = l.getFetchTimestamp();
			if( fetch_ts == null ) {
				fetch_ts = log_ts;
			}
			Date coarse_ts = new Date(HOUR_AS_MILLIS*(l.getLogTimestamp().getTime()/HOUR_AS_MILLIS));
			// Submit to URL lookup:
			BoundStatement boundStatement = new BoundStatement(statement);
			getSession().execute(boundStatement.bind(
					l.getURL(),
					coarse_ts,
					log_ts,
					fetch_ts,
					l.getHTTPCode(),
					l.getSHA1Hash()
					));
			// Submit to log:
			//System.out.println(l.getLogTimestamp().getTime()+" > "+coarse_ts.getTime());
			boundStatement = new BoundStatement(log_statement);
			getSession().execute(boundStatement.bind(
					coarse_ts,
					log_ts,
					l.getURL(),
					fetch_ts,
					l.getHost(),
					l.getDomain(),
					l.getSubdomain(),
					l.getHTTPCode(),
					l.getSHA1Hash(),
					l.getLogId(),
					l.getAnnotations(),
					l.getBreadcrumbCodes(),
					l.getCompressability(),
					l.getContentType(),
					l.getDownloadSize(),
					l.getFetchDuration(),
					l.getReferrer(),
					l.getRetries(),
					l.getWorkerThread()
					));

			AlertType at1 = AlertType.COMPRESSABILITY;
			AlertType at2 = AlertType.MALFORMED_CRAWL_URL;
			AlertType at3 = AlertType.TOO_MANY_PATH_SEGMENTS;
			AlertType at4 = AlertType.TOO_MANY_SUBDOMAINS;
			AlertType at5 = AlertType.TXT_TO_NONTEXT_RATIO;
			for( Alert a : l.getAlerts() ) {
				a.getAlertDescription();
				a.getAlertType();
				a.getOffendingHost();
				a.getTimestamp();
			}

		}
		
	}
	
	private Session getSession() {
		return session;
	}

	public void querySchema() {
		System.out.println("Querying...");
		ResultSet results = session.execute("SELECT * FROM crawl_uris.uris " +
		        "WHERE uri = 'https://www.gov.uk/government/publications';");
		
			for (Row row : results) {
				for( Definition cd : row.getColumnDefinitions()) {
					if( cd.getType() == DataType.text() ) {
 						System.out.println(cd.getName()+": "+row.getString(cd.getName()));
					} else if( cd.getType() == DataType.cint() ) {
 						System.out.println(cd.getName()+": "+row.getInt(cd.getName()));
					} else if( cd.getType() == DataType.bigint() ) {
 						System.out.println(cd.getName()+": "+row.getLong(cd.getName()));
					} else if( cd.getType() == DataType.timestamp() ) {
	 						System.out.println(cd.getName()+": "+row.getDate(cd.getName()));
					} else {
						System.err.println("No case for "+cd.getType());
					}
				}
			}
			System.out.println();
	}
	
	public void dropSchema() {
		session.execute("DROP KEYSPACE crawl_uris");
	}
	
	public void close() {
		   cluster.shutdown();
	}

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {
		
		CassandraIngestTool client = new CassandraIngestTool();
		client.connect("127.0.0.1");
		
		//client.dropSchema();
		if( ! client.isSchemaThere() )  client.createSchema();
		
		//client.loadData();
		
		client.querySchema();
		
		client.close();

	}

}
