package uk.bl.monitrix.database.cassandra.ingest;

import java.util.HashMap;
import java.util.Map;



import com.datastax.driver.core.PreparedStatement;
// import com.datastax.driver.core.BoundStatement;
// import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.database.cassandra.model.CassandraVirusLog;
import uk.bl.monitrix.model.VirusRecord;

public class CassandraVirusLogImporter extends CassandraVirusLog {
	
	private static final String TABLE = CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_VIRUS_LOG;

	private PreparedStatement statement = null;

	public CassandraVirusLogImporter(Session db) {
		super(db);
		
		this.statement = session.prepare(
				"INSERT INTO " + CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_VIRUS_LOG + " (" +
				CassandraProperties.FIELD_VIRUS_LOG_NAME + ", " + CassandraProperties.FIELD_VIRUS_LOG_OCCURENCES + ")" + 
				"VALUES (?, ?);");		
	}

	public void recordOccurence(String virusName, String hostname) {		
		// In this case we know it's a safe cast
		VirusRecord record = (VirusRecord) getRecordForVirus(virusName);
		
		Map<String, Integer> occurences = null;
		if (record == null) {
			occurences = new HashMap<String,Integer>();
		} else {
			occurences = record.getOccurences();
		}
		
		if (occurences.containsKey(hostname)) {
			int count = occurences.get(hostname);
			occurences.put(hostname, count + 1);
		} else {
			occurences.put(hostname, 1);
		}
		
		// BoundStatement boundStatement = new BoundStatement(statement);
		// session.execute(boundStatement.bind(virusName, occurences));		
	}

}
