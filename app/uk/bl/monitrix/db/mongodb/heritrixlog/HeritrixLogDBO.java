package uk.bl.monitrix.db.mongodb.heritrixlog;

import java.util.AbstractList;
import java.util.List;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.DBObject;

/**
 * Wraps the DBObject contained in the MongoDB 'Heritrix Log' collection.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class HeritrixLogDBO {
	
	DBObject dbo;
	
	public HeritrixLogDBO(DBObject dbo) {
		this.dbo = dbo;
	}
	
	/**
	 * The UNIX timestamp of the log entry.
	 * @return the log entry timestamp
	 */
	public long getTimestamp() {
		return (Long) dbo.get(MongoProperties.FIELD_LOG_TIMESTAMP);
	}
	
	/**
	 * Sets the UNIX timestamp of the log entry.
	 * @param timestamp
	 */
	public void setTimestamp(long timestamp) {
		dbo.put(MongoProperties.FIELD_LOG_TIMESTAMP, timestamp);
	}
	
	/**
	 * The host name for this log entry.
	 * @return the host name 
	 */
	public String getHost() {
		return (String) dbo.get(MongoProperties.FIELD_LOG_HOST);
	}
	
	/**
	 * Sets the host name for this log entry.
	 * @param hostname the host name
	 */
	public void setHost(String hostname) {
		dbo.put(MongoProperties.FIELD_LOG_HOST, hostname);
	}
	
	/**
	 * The crawler ID for this log entry.
	 * @return the crawler ID
	 */
	public String getCrawlerID() {
		return (String) dbo.get(MongoProperties.FIELD_LOG_CRAWLER_ID);
	}
	
	/**
	 * Sets the crawler ID for this log entry.
	 * @param crawlerId the crawler ID
	 */
	public void setCrawlerID(String crawlerId) {
		dbo.put(MongoProperties.FIELD_LOG_CRAWLER_ID, crawlerId);
	}
	
	/**
	 * The HTTP/Heritrix return code for this log entry.
	 * @return the HTTP/Heritrix return code
	 */
	public int geHTTPCode() {
		return (Integer) dbo.get(MongoProperties.FIELD_LOG_HTTP_CODE);
	}
	
	/**
	 * Sets the HTTP/Heritrix return code for this log entry.
	 * @param httpCode the HTTP/Heritrix return code
	 */
	public void setHTTPCode(int httpCode) {
		dbo.put(MongoProperties.FIELD_LOG_HTTP_CODE, httpCode);
	}
	
	/**
	 * The original log entry string.
	 * @return the log entry string
	 */
	public String getLogLine() {
		return (String) dbo.get(MongoProperties.FIELD_LOG_LINE);
	}
	
	/**
	 * Sets the original log entry string.
	 * @param line the log entry string
	 */
	public void setLogLine(String line) {
		dbo.put(MongoProperties.FIELD_LOG_LINE, line);
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
