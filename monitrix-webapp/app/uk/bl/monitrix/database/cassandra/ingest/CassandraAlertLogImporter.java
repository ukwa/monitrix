package uk.bl.monitrix.database.cassandra.ingest;

import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.database.cassandra.model.CassandraAlertLog;
import uk.bl.monitrix.heritrix.LogFileEntry.DefaultAlert;
import uk.bl.monitrix.model.CrawlLog;

/**
 * An extended version of {@link CassandraAlertLog} that adds insert capability.
 */
class CassandraAlertLogImporter extends CassandraAlertLog {

	private static long HOUR_IN_MILLIS = 60 * 60 * 1000;
	
	private PreparedStatement statement = null;
	
	public CassandraAlertLogImporter(Session db, CrawlLog log) {
		super(db, log);
		this.statement = session.prepare(
				"INSERT INTO " + CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_ALERT_LOG + " (" +
				CassandraProperties.FIELD_ALERT_LOG_TIMESTAMP + ", " +
				CassandraProperties.FIELD_ALERT_LOG_TIMESTAMP_HR + ", " +
			    CassandraProperties.FIELD_ALERT_LOG_OFFENDING_HOST + ", " +
			    CassandraProperties.FIELD_ALERT_LOG_ALERT_TYPE + ", " + 
				CassandraProperties.FIELD_ALERT_LOG_DESCRIPTION + ") " +
				"VALUES (?, ?, ?, ?, ?);");
	}
	
	public void insert(DefaultAlert alert) {
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(
				alert.getTimestamp(),
				getHour(alert.getTimestamp()),
				alert.getOffendingHost(),
				alert.getAlertType().name(),
				alert.getAlertDescription()));		
	}
	
	public void insert(List<DefaultAlert> alerts) {
		for (DefaultAlert alert : alerts)
			insert(alert);
	}
	
	public static long getHour(long timestamp) {
		// Truncates the timestamp the nearest full hour (due to integer division)
		return (timestamp / HOUR_IN_MILLIS) * HOUR_IN_MILLIS;
	}

}
