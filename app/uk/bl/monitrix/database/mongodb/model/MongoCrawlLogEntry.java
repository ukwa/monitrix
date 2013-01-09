package uk.bl.monitrix.database.mongodb.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.CrawlLogEntry;

/**
 * A MongoDB-backed implementation of {@link CrawlLogEntry}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoCrawlLogEntry extends CrawlLogEntry {
	
	private DBObject dbo;
	
	private List<String> fields = null;
	
	public MongoCrawlLogEntry(DBObject dbo) {
		this.dbo = dbo;
	}
	
	private void parseEntry() {
		fields = new ArrayList<String>();
		for (String field : this.toString().split(" ")) {
			if (!field.isEmpty())
				fields.add(field.trim());
		}
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
		return new Date((Long) dbo.get(MongoProperties.FIELD_CRAWL_LOG_TIMESTAMP));
	}

	public void setTimestamp(long timestamp) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_TIMESTAMP, timestamp);
	}

	@Override
	public int getHTTPCode() {
		return (Integer) dbo.get(MongoProperties.FIELD_CRAWL_LOG_HTTP_CODE);
	}
	
	public void setHTTPCode(int httpCode) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_HTTP_CODE, httpCode);
	}

	@Override
	public int getDownloadSize() {
		if (fields == null)
			parseEntry();
		
		if (fields.get(2).equals("-"))
			return 0;
		
		return Integer.parseInt(fields.get(2));
	}

	@Override
	public String getURL() {
		if (fields == null)
			parseEntry();
		
		return fields.get(3);
	}

	@Override
	public String getHost() {
		return (String) dbo.get(MongoProperties.FIELD_CRAWL_LOG_HOST);
	}
	
	public void setHost(String hostname) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_HOST, hostname);
	}

	@Override
	public String getBreadcrumbCodes() {
		if (fields == null)
			parseEntry();
		
		return fields.get(4);
	}

	@Override
	public String getReferrer() {
		if (fields == null)
			parseEntry();
		
		return fields.get(5);
	}

	@Override
	public String getContentType() {
		if (fields == null)
			parseEntry();
		
		return fields.get(6);
	}

	@Override
	public String getCrawlerID() {
		return (String) dbo.get(MongoProperties.FIELD_CRAWL_LOG_CRAWLER_ID);
	}
	
	public void setCrawlerID(String crawlerId) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_CRAWLER_ID, crawlerId);
	}

	@Override
	public String getSHA1Hash() {
		if (fields == null)
			parseEntry();
		
		return fields.get(9);
	}

	@Override
	public String getAnnotations() {
		if (fields == null)
			parseEntry();
		
		return fields.get(11);
	}
	
	public void setLogLine(String line) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_LINE, line);
	}
	
	@Override
	public String toString() {
		return (String) dbo.get(MongoProperties.FIELD_CRAWL_LOG_LINE);
	}

}
