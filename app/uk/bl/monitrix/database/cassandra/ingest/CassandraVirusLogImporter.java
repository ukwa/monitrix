package uk.bl.monitrix.database.cassandra.ingest;

import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;

import uk.bl.monitrix.database.mongodb.model.MongoVirusLog;
import uk.bl.monitrix.database.mongodb.model.MongoVirusRecord;

public class CassandraVirusLogImporter extends MongoVirusLog {

	public CassandraVirusLogImporter(DB db) {
		super(db);
	}

	/**
	 * TODO this class should have caching as well.
	 * Main problem is the escaping/unescaping of hostnames that takes place on every update.
	 */
	public void recordOccurence(String virusName, String hostname) {
		// In this case we know it's a safe cast
		MongoVirusRecord record = (MongoVirusRecord) getRecordForVirus(virusName);
		if (record == null) {
			record = new MongoVirusRecord(new BasicDBObject());
			record.setName(virusName);
		}
		
		Map<String, Integer> occurences = record.getOccurences();
		if (occurences.containsKey(hostname)) {
			int count = occurences.get(hostname);
			occurences.put(hostname, count + 1);
		} else {
			occurences.put(hostname, 1);
		}
		record.setOccurences(occurences);
		
		collection.save(record.getBackingDBO());
	}

}
