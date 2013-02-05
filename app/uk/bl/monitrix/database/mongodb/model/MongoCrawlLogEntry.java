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
		String[] split = this.toString().split(" ");

		// Column 1 - 11
		int ctr = 0;
		while (fields.size() < 11 && ctr < split.length) {
			if (!split[ctr].isEmpty())
				fields.add(split[ctr].trim());
			ctr++;
		}
		
		// Column 12 (annotations) - note that annotations may contain white spaces, so we need to re-join
		StringBuilder sb = new StringBuilder();
		for (int i=ctr; i<split.length; i++) {
			sb.append(split[i] + " ");
		}
		
		fields.add(sb.toString().trim());
	}
	
	/**
	 * Returns the MongoDB entity that's backing this object.
	 * @return the DBObject
	 */
	public DBObject getBackingDBO() {
		return dbo;
	}
	
	@Override
	public String getLogPath() {
		return (String) dbo.get(MongoProperties.FIELD_CRAWL_LOG_LOG_PATH);
	}
	
	public void setLogPath(String logPath) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_LOG_PATH, logPath);
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
	public String getSubdomain() {
		return (String) dbo.get(MongoProperties.FIELD_CRAWL_LOG_SUBDOMAIN);
	}
	
	public void setSubdomain(String subdomain) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_SUBDOMAIN, subdomain);
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
		return (String) dbo.get(MongoProperties.FIELD_CRAWL_LOG_ANNOTATIONS);
	}
	
	public void setAnnotations(String annotations) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_ANNOTATIONS, annotations);
	}
	
	public void setLogLine(String line) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_LINE, line);
	}
	
	@Override
	public String toString() {
		return (String) dbo.get(MongoProperties.FIELD_CRAWL_LOG_LINE);
	}

}
