package uk.bl.monitrix.database.mongodb.model;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
	
	private static DateFormat RFC2550_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	
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
	public String getLogId() {
		return (String) dbo.get(MongoProperties.FIELD_CRAWL_LOG_LOG_ID);
	}
	
	public void setLogId(String logId) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_LOG_ID, logId);
	}

	@Override
	public Date getLogTimestamp() {
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
	public long getDownloadSize() {
		if (fields == null)
			parseEntry();
		
		if (fields.get(2).equals("-"))
			return 0;
		
		return Long.parseLong(fields.get(2));
	}

	@Override
	public String getURL() {
		return (String) dbo.get(MongoProperties.FIELD_CRAWL_LOG_URL);
	}
	
	public void setURL(String url) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_URL, url);
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
	public String getWorkerThread() {
		return (String) dbo.get(MongoProperties.FIELD_CRAWL_LOG_CRAWLER_ID);
	}
	
	@Override
	public Date getFetchTimestamp() {
		if (fields == null)
			parseEntry();
		
		try {
			String timestamp = fields.get(8);
			if (timestamp.indexOf('+') > -1)
				timestamp = timestamp.substring(0, timestamp.indexOf('+'));
			
			System.out.println("fetch timestamp: " + timestamp);
			return RFC2550_FORMAT.parse(timestamp);
		} catch (ParseException e) {
			// Should never happen!
			throw new RuntimeException(e);
		}
	}

	@Override
	public int getFetchDuration() {
		if (fields == null)
			parseEntry();
		
		String duration = fields.get(8);
		if (duration.indexOf('+') > -1) {
			duration = duration.substring(duration.indexOf('+') + 1);
			return Integer.parseInt(duration);
		}
		
		return 0;
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
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_ANNOTATIONS_TOKENIZED, Arrays.asList(annotations.split(",")));
	}
	
	@Override
	public int getRetries() {
		return (Integer) dbo.get(MongoProperties.FIELD_CRAWL_LOG_RETRIES);
	}

	public void setRetries(int retries) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_RETRIES, retries);
	}
	
	@Override
	public double getCompressability() {
		return (Double) dbo.get(MongoProperties.FIELD_CRAWL_LOG_COMPRESSABILITY);
	}

	public void setCompressability(double compressability) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_COMPRESSABILITY, compressability);
	}
	
	public void setLogLine(String line) {
		dbo.put(MongoProperties.FIELD_CRAWL_LOG_LINE, line);
	}
		
	@Override
	public String toString() {
		return (String) dbo.get(MongoProperties.FIELD_CRAWL_LOG_LINE);
	}

}
