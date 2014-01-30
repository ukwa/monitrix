package uk.bl.monitrix.database.mongodb.model;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.Alert;

import com.mongodb.DBObject;

/**
 * An implementation of {@link Alert} backed by MongoDB.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoAlert implements Alert {

	private DBObject dbo;
	
	public MongoAlert(DBObject dbo) {
		this.dbo = dbo;
	}
	
	/**
	 * Returns the MongoDB entity that's backing this object.
	 * @return the DBObject
	 */
	public DBObject getBackingDBO() {
		return dbo;
	}
	
	@Override
	public long getTimestamp() {
		return (Long) dbo.get(MongoProperties.FIELD_ALERT_LOG_TIMESTAMP);
	}
	
	public void setTimestamp(long timestamp) {
		dbo.put(MongoProperties.FIELD_ALERT_LOG_TIMESTAMP, timestamp);
	}
	
	@Override
	public String getOffendingHost() {
		return dbo.get(MongoProperties.FIELD_ALERT_LOG_OFFENDING_HOST).toString();
	}
	
	public void setOffendingHost(String hostname) {
		dbo.put(MongoProperties.FIELD_ALERT_LOG_OFFENDING_HOST, hostname);
	}
	
	@Override
	public AlertType getAlertType() {
		return AlertType.valueOf(dbo.get(MongoProperties.FIELD_ALERT_LOG_ALERT_TYPE).toString());
	}
	
	public void setAlertType(AlertType alertType) {
		dbo.put(MongoProperties.FIELD_ALERT_LOG_ALERT_TYPE, alertType.name());
	}
	
	@Override
	public String getAlertDescription() {
		return dbo.get(MongoProperties.FIELD_ALERT_LOG_DESCRIPTION).toString();
	}
	
	public void setAlertDescription(String description) {
		dbo.put(MongoProperties.FIELD_ALERT_LOG_DESCRIPTION, description);
	}
	
}
