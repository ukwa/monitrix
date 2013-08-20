package uk.bl.monitrix.database.cassandra.model;

import java.util.Iterator;

import com.datastax.driver.core.Session;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.VirusLog;
import uk.bl.monitrix.model.VirusRecord;

public class CassandraVirusLog implements VirusLog {
	
	protected DBCollection collection;
	
	public CassandraVirusLog(Session session) {
		this.collection = session.getCollection(MongoProperties.COLLECTION_VIRUS_LOG);
		
		// Virus Log collection is indexed by virus name
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_VIRUS_LOG_NAME, 1));
	}
	
	@Override
	public VirusRecord getRecordForVirus(String virusName) {
		DBObject dbo = collection.findOne(new BasicDBObject(MongoProperties.FIELD_VIRUS_LOG_NAME, virusName));
		if (dbo == null)
			return null;
		
		return new MongoVirusRecord(dbo);
	}

	@Override
	public Iterator<VirusRecord> getVirusRecords() {
		final DBCursor cursor = collection.find();
		return new Iterator<VirusRecord>() {
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public VirusRecord next() {
				return new MongoVirusRecord(cursor.next());
			}

			@Override
			public void remove() {
				cursor.remove();				
			}
		};
	}

}
