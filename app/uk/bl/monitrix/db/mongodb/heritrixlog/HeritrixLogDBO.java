package uk.bl.monitrix.db.mongodb.heritrixlog;

import java.util.AbstractList;
import java.util.List;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.DBObject;

public class HeritrixLogDBO {
	
	DBObject dbo;
	
	public HeritrixLogDBO(DBObject dbo) {
		this.dbo = dbo;
	}
	
	public long getTimestamp() {
		return (Long) dbo.get(MongoProperties.FIELD_LOG_TIMESTAMP);
	}
	
	public void setTimestamp(long timestamp) {
		dbo.put(MongoProperties.FIELD_LOG_TIMESTAMP, timestamp);
	}
	
	public String getLogLine() {
		return (String) dbo.get(MongoProperties.FIELD_LOG_LINE);
	}
	
	public void setLogLine(String line) {
		dbo.put(MongoProperties.FIELD_LOG_LINE, line);
	}
	
	public String getHost() {
		return (String) dbo.get(MongoProperties.FIELD_LOG_HOST);
	}
	
	public void setHost(String hostname) {
		dbo.put(MongoProperties.FIELD_LOG_HOST, hostname);
	}
	
	public String getCrawlerID() {
		return (String) dbo.get(MongoProperties.FIELD_LOG_CRAWLER_ID);
	}
	
	public void setCrawlerID(String crawlerId) {
		dbo.put(MongoProperties.FIELD_LOG_CRAWLER_ID, crawlerId);
	}
	
	public int geHTTPCode() {
		return (Integer) dbo.get(MongoProperties.FIELD_LOG_HTTP_CODE);
	}
	
	public void setHTTPCode(int httpCode) {
		dbo.put(MongoProperties.FIELD_LOG_HTTP_CODE, httpCode);
	}
	
	/**
	 * A utility method that lazily maps a list of wrapper objects to a (read-only)
	 * list of MongoDB DBObjects. Quick'n'dirty replacement for Scala's (very convenient...)
	 * built-in .map method! 
	 * @param log the list of wrapper objects
	 * @return the list of DBObjects
	 */
	static List<DBObject> map(final List<HeritrixLogDBO> log) {
		return new AbstractList<DBObject>() {
			
			@Override
			public DBObject get(int index) {
				return log.get(index).dbo;
			}

			@Override
			public int size() {
				return log.size();
			}
			
		};
	}

}
