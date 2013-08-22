package uk.bl.monitrix.database.cassandra.ingest;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.model.CassandraVirusLog;
import uk.bl.monitrix.heritrix.LogFileEntry.DefaultVirusRecord;
import uk.bl.monitrix.model.VirusRecord;

public class CassandraVirusLogImporter extends CassandraVirusLog {

	private PreparedStatement statement;

	public CassandraVirusLogImporter(Session db) {
		super(db);
		statement = session.prepare(
			      "INSERT INTO crawl_uris.virus_log " +
			      "(virus_name, occurences) " +
			      "VALUES (?, ?);");
		
	}
	
	private void insert(DefaultVirusRecord defaultVirusRecord) {
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(
				defaultVirusRecord.getName(),
				defaultVirusRecord.getOccurences()
				));
	}

	/**
	 */
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
		
		this.insert(new DefaultVirusRecord(virusName,occurences));
	}

}
