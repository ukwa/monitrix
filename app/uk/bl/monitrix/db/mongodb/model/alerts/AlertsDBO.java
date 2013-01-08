package uk.bl.monitrix.db.mongodb.model.alerts;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.DBObject;

public class AlertsDBO {

	DBObject dbo;
	
	public AlertsDBO(DBObject dbo) {
		this.dbo = dbo;
	}
	
	public String getOffendingHost() {
		return dbo.get(MongoProperties.FIELD_ALERTS_OFFENDING_HOST).toString();
	}
	
	public void setOffendingHost(String hostname) {
		dbo.put(MongoProperties.FIELD_ALERTS_OFFENDING_HOST, hostname);
	}
	
	public String getAlertName() {
		return dbo.get(MongoProperties.FIELD_ALERTS_NAME).toString();
	}
	
	public void setAlertName(String alertName) {
		dbo.put(MongoProperties.FIELD_ALERTS_NAME, alertName);
	}
	
	public String getAlertDescription() {
		return dbo.get(MongoProperties.FIELD_ALERTS_DESCRIPTION).toString();
	}
	
	public void setAlertDescription(String description) {
		dbo.put(MongoProperties.FIELD_ALERTS_DESCRIPTION, description);
	}
	
}
