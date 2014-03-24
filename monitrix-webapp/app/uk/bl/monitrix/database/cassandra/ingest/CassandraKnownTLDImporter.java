package uk.bl.monitrix.database.cassandra.ingest;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.KnownHostList;

/**
 * A CassandraDB-backed helper to the {@link KnownHostList}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraKnownTLDImporter {
	
	private static final String TABLE_TLDS = CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_KNOWN_TLDS;

	// A simple in-memory buffer for quick TLD lookups
	protected Map<String, Long> cache = new HashMap<String, Long>();
	
	protected Session session;
	
	private PreparedStatement statement = null;

	public CassandraKnownTLDImporter(Session session) {
		this.session = session;
		
		this.statement = session.prepare(
				"INSERT INTO " + CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_KNOWN_TLDS + " (" +
				CassandraProperties.FIELD_KNOWN_TLDS_TLD + ", " + CassandraProperties.FIELD_KNOWN_TLDS_COUNT + ")" + 
				"VALUES (?, ?);");		
	}
	
	private long getOrCreate(String tld) {
		ResultSet results = session.execute("SELECT * FROM " + TABLE_TLDS + " WHERE " + CassandraProperties.FIELD_KNOWN_TLDS_TLD + "='" + tld + "';"); 
		if (results.isExhausted()) {
			BoundStatement boundStatement = new BoundStatement(statement);
			session.execute(boundStatement.bind(tld, 0l));
			return 0l;
		} else {
			Row row = results.one();
			return row.getLong(CassandraProperties.FIELD_KNOWN_TLDS_COUNT);
		}
	}
	
	public void incrementTLDCount(String tld) {
		Long cachedCount = cache.get(tld);
		if (cachedCount == null) {
			cachedCount = getOrCreate(tld);
		} else {
			cachedCount = cachedCount + 1;
		}
		cache.put(tld, cachedCount);
	}
	
	public void commit() {
		for (Entry<String, Long> e : cache.entrySet()) {
			session.execute(
					"UPDATE " + TABLE_TLDS + " SET " + CassandraProperties.FIELD_KNOWN_TLDS_COUNT + "=" + e.getValue() +
					" WHERE " + CassandraProperties.FIELD_KNOWN_TLDS_TLD + "='" + e.getKey() + "';");
		}
	}
}
