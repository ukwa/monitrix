package uk.bl.monitrix.database.cassandra.ingest;

import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.database.cassandra.model.CassandraAlertLog;
import uk.bl.monitrix.heritrix.LogFileEntry.DefaultAlert;

/**
 * An extended version of {@link CassandraAlertLog} that adds insert capability.
 */
class CassandraAlertLogImporter extends CassandraAlertLog {

	private PreparedStatement statement = null;
	
	public CassandraAlertLogImporter(Session db) {
		super(db);
		this.statement = session.prepare(
				"INSERT INTO " + CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_ALERT_LOG + " (" +
				CassandraProperties.FIELD_ALERT_LOG_TIMESTAMP + ", " +
			    CassandraProperties.FIELD_ALERT_LOG_OFFENDING_HOST + ", " +
			    CassandraProperties.FIELD_ALERT_LOG_ALERT_TYPE + ", " + 
				CassandraProperties.FIELD_ALERT_LOG_DESCRIPTION + ") " +
				"VALUES (?, ?, ?, ?, ?);");
	}
	
	public void insert(DefaultAlert alert) {
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(
				alert.getTimestamp(),
				alert.getOffendingHost(),
				alert.getAlertType().name(),
				alert.getAlertDescription()));		
	}
	
	public void insert(List<DefaultAlert> alerts) {
		for (DefaultAlert alert : alerts)
			insert(alert);
	}

}
