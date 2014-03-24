package uk.bl.monitrix.database.cassandra.ingest;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.database.cassandra.model.CassandraVirusLog;
import uk.bl.monitrix.model.VirusRecord;

public class CassandraVirusLogImporter extends CassandraVirusLog {

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
		Map<String, Integer> occurrences = record.getOccurences();
		
		Integer count = occurrences.get(hostname);
		if (count == null) {
			count = 1;
		} else {
			count = count + 1;
		}
		occurrences.put(hostname, count);
		
		try {
			BoundStatement boundStatement = new BoundStatement(statement);
			session.execute(boundStatement.bind(virusName, toJson(occurrences)));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private String toJson(Map<String, Integer> occurrences) throws JsonGenerationException, JsonMappingException, IOException {
		StringWriter writer = new StringWriter();
		new ObjectMapper().writeValue(writer, occurrences);
		return writer.toString().replace("'", "''");
	}

}
