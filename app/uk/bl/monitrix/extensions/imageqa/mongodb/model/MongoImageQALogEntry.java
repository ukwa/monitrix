package uk.bl.monitrix.extensions.imageqa.mongodb.model;

import uk.bl.monitrix.extensions.imageqa.model.ImageQALogEntry;
import uk.bl.monitrix.extensions.imageqa.mongodb.ImageQAMongoProperties;

import com.mongodb.DBObject;

public class MongoImageQALogEntry implements ImageQALogEntry {
	
	private DBObject dbo;
	
	public MongoImageQALogEntry(DBObject dbo) {
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
	public String getOriginalWebURL() {
		return (String) dbo.get(ImageQAMongoProperties.FIELD_IMAGE_QA_LOG_ORIGINAL_WEB_URL);
	}
	
	public void setOriginalWebURL(String url) {
		dbo.put(ImageQAMongoProperties.FIELD_IMAGE_QA_LOG_ORIGINAL_WEB_URL, url);
	}

	@Override
	public String getWaybackImageURL() {
		return (String) dbo.get(ImageQAMongoProperties.FIELD_IMAGE_QA_LOG_WAYBACK_IMAGE_URL);
	}
	
	public void setWaybackImageURL(String url) {
		dbo.put(ImageQAMongoProperties.FIELD_IMAGE_QA_LOG_WAYBACK_IMAGE_URL, url);
	}

	@Override
	public String getOriginalImageURL() {
		return (String) dbo.get(ImageQAMongoProperties.FIELD_IMAGE_QA_LOG_ORIGINAL_IMAGE_URL);
	}
	
	public void setOriginalImageURL(String url) {
		dbo.put(ImageQAMongoProperties.FIELD_IMAGE_QA_LOG_ORIGINAL_IMAGE_URL, url);
	}

	@Override
	public String getMessage() {
		return (String) dbo.get(ImageQAMongoProperties.FIELD_IMAGE_QA_LOG_MESSAGE);
	}

	public void setMessage(String message) {
		dbo.put(ImageQAMongoProperties.FIELD_IMAGE_QA_LOG_MESSAGE, message);
	}
	
	@Override
	public String getPSNRMessage() {
		return (String) dbo.get(ImageQAMongoProperties.FIELD_IMAGE_QA_LOG_PSNR_MESSAGE);
	}
	
	public void setPSNRMessage(String message) {
		dbo.put(ImageQAMongoProperties.FIELD_IMAGE_QA_LOG_PSNR_MESSAGE, message);
	}
	
	public String toString() {
		return (String) dbo.get(ImageQAMongoProperties.FIELD_IMAGE_QA_LOG_LINE);
	}
	
	public void setLogLine(String line) {
		dbo.put(ImageQAMongoProperties.FIELD_IMAGE_QA_LOG_LINE, line);
	}

}
