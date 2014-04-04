package uk.bl.monitrix.database.cassandra.model;

import com.datastax.driver.core.Row;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.Alert;

/**
 * An implementation of {@link Alert} backed by CassandraDB.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraAlert implements Alert {

	private Row row;
	
	public CassandraAlert(Row row) {
		this.row = row;
	}
	
	@Override
	public long getTimestamp() {
		return row.getLong(CassandraProperties.FIELD_ALERT_LOG_TIMESTAMP);
	}
	
	@Override
	public String getOffendingHost() {
		return row.getString(CassandraProperties.FIELD_ALERT_LOG_OFFENDING_HOST).toString();
	}
	
	@Override
	public AlertType getAlertType() {
		return AlertType.valueOf(row.getString(CassandraProperties.FIELD_ALERT_LOG_ALERT_TYPE).toString());
	}
	
	@Override
	public String getAlertDescription() {
		return row.getString(CassandraProperties.FIELD_ALERT_LOG_DESCRIPTION).toString();
	}
		
}