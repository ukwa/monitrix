package uk.bl.monitrix.database.cassandra.model;

import java.util.Iterator;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.VirusLog;
import uk.bl.monitrix.model.VirusRecord;

public class CassandraVirusLog implements VirusLog {
	
	private final String TABLE_VIRUS = CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_VIRUS_LOG;
	
	protected Session session;
	
	public CassandraVirusLog(Session session) {
		this.session = session;
	}
	
	@Override
	public VirusRecord getRecordForVirus(String virusName) {
		// TODO it may make sense to cache this
		ResultSet results =
				session.execute("SELECT * FROM " + TABLE_VIRUS + " WHERE " + CassandraProperties.FIELD_VIRUS_LOG_NAME +	"='" + virusName + "';");
		
		if (results.isExhausted())
			return null;
		
		return new CassandraVirusRecord(results.one());
	}

	@Override
	public Iterator<VirusRecord> getVirusRecords() {
		ResultSet results = session.execute("SELECT * FROM " + TABLE_VIRUS + ";");
		final Iterator<Row> cursor = results.iterator();
		return new Iterator<VirusRecord>() {
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public VirusRecord next() {
				return new CassandraVirusRecord(cursor.next());
			}

			@Override
			public void remove() {
				cursor.remove();				
			}
		};
	}

}
