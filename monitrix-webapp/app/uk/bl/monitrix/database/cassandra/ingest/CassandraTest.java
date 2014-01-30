/**
 * 
 */
package uk.bl.monitrix.database.cassandra.ingest;

import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import uk.bl.monitrix.heritrix.LogFileEntry;
import uk.bl.monitrix.heritrix.SimpleLogfileReader;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class CassandraTest {
	
	private Cluster cluster;
	
	private Session session;
	
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
	
	public void createSchema() {
		session.execute("CREATE KEYSPACE simplex WITH replication " + 
			      "= {'class':'SimpleStrategy', 'replication_factor':1};");
		
		session.execute(
			      "CREATE TABLE simplex.songs (" +
			            "id uuid PRIMARY KEY," + 
			            "title text," + 
			            "album text," + 
			            "artist text," + 
			            "tags set<text>," + 
			            "data blob" + 
			            ");");
		
		session.execute(
			      "CREATE TABLE simplex.playlists (" +
			            "id uuid," +
			            "title text," +
			            "album text, " + 
			            "artist text," +
			            "song_id uuid," +
			            "PRIMARY KEY (id, title, album, artist)" +
			            ");");
	
	}
	
	public void loadData() {
		PreparedStatement statement = getSession().prepare(
			      "INSERT INTO simplex.songs " +
			      "(id, title, album, artist, tags) " +
			      "VALUES (?, ?, ?, ?, ?);");
		
		BoundStatement boundStatement = new BoundStatement(statement);
		Set<String> tags = new HashSet<String>();
		tags.add("jazz");
		tags.add("2013");
		getSession().execute(boundStatement.bind(
		      UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
		      "La Petite Tonkinoise'",
		      "Bye Bye Blackbird'",
		      "Joséphine Baker",
		      tags ) );
		
		
		statement = getSession().prepare(
			      "INSERT INTO simplex.playlists " +
			      "(id, song_id, title, album, artist) " +
			      "VALUES (?, ?, ?, ?, ?);");
			boundStatement = new BoundStatement(statement);
			getSession().execute(boundStatement.bind(
			      UUID.fromString("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
			      UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
			      "La Petite Tonkinoise",
			      "Bye Bye Blackbird",
			      "Joséphine Baker") );
			
	}
	
	private Session getSession() {
		return session;
	}

	public void querySchema() {
		ResultSet results = session.execute("SELECT * FROM simplex.playlists " +
		        "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
		
		System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
			       "-------------------------------+-----------------------+--------------------"));
			for (Row row : results) {
			    System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
			    row.getString("album"),  row.getString("artist")));
			}
			System.out.println();
	}
	
	public void close() {
		   cluster.shutdown();
	}

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {
		
		CassandraTest client = new CassandraTest();
		client.connect("127.0.0.1");
		   client.createSchema();
		   client.loadData();
		   client.querySchema();
//		   client.updateSchema();
//		   client.dropSchema();
		   /*
		    * DROP KEYSPACE XXX;
		    */
		   client.close();

/*		
		SimpleLogfileReader slfr = new SimpleLogfileReader("/Users/andy/Documents/workspace/bl-crawler-tests/heritrix-3.1.2-SNAPSHOT/jobs/bl-test-crawl/heritrix/output/logs/bl-test-crawl/crawl.log.cp00001-20130605082749");
		Iterator<LogFileEntry> lines = slfr.iterator();
		while( lines.hasNext() ) {
			LogFileEntry l = lines.next();
			// SURT surt = SURT.fromURI();
			// AND canonicalised.
			updater = template.createUpdater(l.getURL());
			updater.setString("domain", l.getDomain());
			updater.setString("host", l.getHost());
			updater.setString("sha1", l.getSHA1Hash());
			updater.setInteger("status_code",l.getHTTPCode());
			if( l.getFetchTimestamp() != null ) {
				updater.setLong("fetch_time", l.getFetchTimestamp().getTime());
			}
			try {
			    template.update(updater);
			} catch (HectorException e) {
			    // do something ...
				e.printStackTrace();
			}
		}
		*/

	}

}
