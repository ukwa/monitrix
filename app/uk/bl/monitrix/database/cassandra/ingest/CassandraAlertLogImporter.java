package uk.bl.monitrix.database.cassandra.ingest;

import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.model.CassandraAlert;
import uk.bl.monitrix.database.cassandra.model.CassandraAlertLog;
import uk.bl.monitrix.heritrix.LogFileEntry.DefaultAlert;

/**
 * An extended version of {@link CassandraAlertLog} that adds insert capability.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class CassandraAlertLogImporter extends CassandraAlertLog {

	PreparedStatement statement = null;
	
	public CassandraAlertLogImporter(Session db) {
		super(db);
		this.statement = session.prepare(
			      "INSERT INTO crawl_uris.alerts " +
					      "(host, alert_ts, alert_type, description) " +
					      "VALUES (?, ?, ?, ?);");
	}
	
	public void insert(DefaultAlert alert) {
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(
				alert.getOffendingHost(),
				alert.getTimestamp(),
				alert.getAlertType(),
				alert.getAlertDescription()
				));
	}
	
	public void insert(final List<DefaultAlert> alerts) {
		for( DefaultAlert alert : alerts ) {
			this.insert(alert);
		}
	}

}
