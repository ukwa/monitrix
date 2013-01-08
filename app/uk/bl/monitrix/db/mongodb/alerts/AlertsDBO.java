package uk.bl.monitrix.db.mongodb.alerts;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.DBObject;

public class AlertsDBO {

	DBObject dbo;
	
	public AlertsDBO(DBObject dbo) {
		this.dbo = dbo;
	}
	
	public AlertType getAlertType() {
		return AlertType.valueOf(dbo.get(MongoProperties.FIELD_ALERTS_TYPE).toString());
	}
	
	public void setAlertType(AlertType alertType) {
		dbo.put(MongoProperties.FIELD_ALERTS_TYPE, alertType.name());
	}
	
	public String getURL() {
		return dbo.get(MongoProperties.FIELD_ALERTS_URL).toString();
	}
	
	public void setURL(String url) {
		dbo.put(MongoProperties.FIELD_ALERTS_URL, url);
	}
	
}
