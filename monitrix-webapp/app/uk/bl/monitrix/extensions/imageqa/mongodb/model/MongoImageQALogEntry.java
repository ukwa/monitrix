package uk.bl.monitrix.extensions.imageqa.mongodb.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import uk.bl.monitrix.extensions.imageqa.model.ImageQALogEntry;
import uk.bl.monitrix.extensions.imageqa.mongodb.MongoImageQAProperties;

import com.mongodb.DBObject;

public class MongoImageQALogEntry implements ImageQALogEntry {
	
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
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
	public Date getTimestamp() {
		String timestamp = (String) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_TIMESTAMP);
		try {
			return DATE_FORMAT.parse(timestamp);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void setTimestamp(Date date) {
		String timestamp = DATE_FORMAT.format(date);
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_TIMESTAMP, timestamp);
	}

	@Override
	public double getExecutionTime() {
		return (Double) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_EXECUTION_TIME);
	}
	
	public void setExecutionTime(double time) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_EXECUTION_TIME, time);
	}
	
	@Override
	public String getOriginalWebURL() {
		return (String) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_ORIGINAL_WEB_URL);
	}
	
	public void setOriginalWebURL(String url) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_ORIGINAL_WEB_URL, url);
	}

	@Override
	public String getWaybackImageURL() {
		return (String) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_WAYBACK_IMAGE_URL);
	}
	
	public void setWaybackImageURL(String url) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_WAYBACK_IMAGE_URL, url);
	}

	@Override
	public long getWaybackTimestamp() {
		return (Long) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_WAYBACK_TIMESTAMP);
	}
	
	public void setWaybackTimestamp(long timestamp) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_WAYBACK_TIMESTAMP, timestamp);
	}

	@Override
	public int getFC1() {
		return (Integer) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_FC1);
	}
	
	public void setFC1(int fc1) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_FC1, fc1);
	}

	@Override
	public int getFC2() {
		return (Integer) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_FC2);
	}

	public void setFC2(int fc2) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_FC2, fc2);		
	}
	
	@Override
	public int getMC() {
		return (Integer) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_MC);
	}
	
	public void setMC(int mc) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_MC, mc);		
	}
	
	@Override
	public String getMessage() {
		return (String) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_MESSAGE);
	}

	public void setMessage(String message) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_MESSAGE, message);
	}
	@Override
	public int getTS1() {
		return (Integer) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_TS1);
	}
	
	public void setTS1(int ts1) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_TS1, ts1);				
	}

	@Override
	public int getTS2() {
		return (Integer) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_TS2);
	}
	
	public void setTS2(int ts2) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_TS2, ts2);				
	}

	@Override
	public int getOCR() {
		return (Integer) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_OCR);
	}
	
	public void setOCR(int ocr) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_OCR, ocr);						
	}

	@Override
	public int getImage1Size() {
		return (Integer) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_IMG1_SIZE);
	}
	
	public void setImage1Size(int size) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_IMG1_SIZE, size);								
	}

	@Override
	public int getImage2Size() {
		return (Integer) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_IMG2_SIZE);
	}
	
	public void setImage2Size(int size) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_IMG2_SIZE, size);								
	}

	@Override
	public double getPSNRSimilarity() {
		return (Double) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_PSNR_SIMILARITY);
	}
	
	public void setPSNRSimilarity(double similarity) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_PSNR_SIMILARITY, similarity);								
	}

	@Override
	public double getPSNRThreshold() {
		return (Double) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_PSNR_THRESHOLD);
	}
	
	public void setPSNRThreshold(double threshold) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_PSNR_THRESHOLD, threshold);								
	}
	
	@Override
	public String getPSNRMessage() {
		return (String) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_PSNR_MESSAGE);
	}
	
	public void setPSNRMessage(String message) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_PSNR_MESSAGE, message);
	}
		
	@Override
	public String getOriginalImageURL() {
		return (String) dbo.get(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_ORIGINAL_IMAGE_URL);
	}
	
	public void setOriginalImageURL(String url) {
		dbo.put(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_ORIGINAL_IMAGE_URL, url);
	}
	
}
