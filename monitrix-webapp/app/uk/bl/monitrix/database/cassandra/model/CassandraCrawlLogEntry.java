package uk.bl.monitrix.database.cassandra.model;

import java.util.Date;

import com.datastax.driver.core.Row;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.CrawlLogEntry;

/**
 * A CassandraDB-backed implementation of {@link CrawlLogEntry}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraCrawlLogEntry extends CrawlLogEntry {
	
	private Row row;
	
	public CassandraCrawlLogEntry(Row row) {
		this.row = row;
	}
	
	@Override
	public String getLogId() {
		return (String) row.getString(CassandraProperties.FIELD_CRAWL_LOG_LOG_ID);
	}
	
	@Override
	public Date getLogTimestamp() {
		return row.getDate(CassandraProperties.FIELD_CRAWL_LOG_TIMESTAMP);
	}

	@Override
	public int getHTTPCode() {
		return row.getInt(CassandraProperties.FIELD_CRAWL_LOG_STATUS_CODE);
	}
	
	@Override
	public long getDownloadSize() {
		long size = row.getLong("download_size");
		//if (fields.get(2).equals("-"))
		//	return 0;
		
		return size; //Long.parseLong(fields.get(2));
	}

	@Override
	public String getURL() {
		return row.getString(CassandraProperties.FIELD_CRAWL_LOG_URL);
	}
	
	@Override
	public String getHost() {
		return row.getString(CassandraProperties.FIELD_CRAWL_LOG_HOST);
	}
	
	@Override
	public String getDomain() {
		return row.getString(CassandraProperties.FIELD_CRAWL_LOG_DOMAIN);
	}
	
	@Override
	public String getSubdomain() {
		return row.getString(CassandraProperties.FIELD_CRAWL_LOG_SUBDOMAIN);
	}
	
	@Override
	public String getBreadcrumbCodes() {
		return row.getString("discovery_path");
	}

	@Override
	public String getReferrer() {
		return row.getString("referer");
	}

	@Override
	public String getContentType() {
		return row.getString("content_type");
	}

	@Override
	public String getWorkerThread() {
		return row.getString(CassandraProperties.FIELD_CRAWL_LOG_CRAWLER_ID);
	}
	
	@Override
	public Date getFetchTimestamp() {
		return row.getDate("fetch_ts");
	}

	@Override
	public int getFetchDuration() {
		return row.getInt("fetch_duration");
	}
	
	@Override
	public String getSHA1Hash() {
		return row.getString("hash");
	}

	@Override
	public String getAnnotations() {
		return row.getString(CassandraProperties.FIELD_CRAWL_LOG_ANNOTATIONS);
	}
	
	@Override
	public int getRetries() {
		// TODO
		// return row.getInt(CassandraProperties.FIELD_CRAWL_LOG_RETRIES);
		return 0;
	}

	@Override
	public double getCompressability() {
		// TODO 
		// return row.getDouble(CassandraProperties.FIELD_CRAWL_LOG_COMPRESSABILITY);
		return 0;
	}

	@Override
	public String toString() {
		return row.getString(CassandraProperties.FIELD_CRAWL_LOG_LINE);
	}

}
