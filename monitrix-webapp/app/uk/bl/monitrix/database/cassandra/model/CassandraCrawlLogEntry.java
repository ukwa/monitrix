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
		return new Date(row.getLong(CassandraProperties.FIELD_CRAWL_LOG_LONG_TIMESTAMP));
	}

	@Override
	public int getHTTPCode() {
		return row.getInt(CassandraProperties.FIELD_CRAWL_LOG_STATUS_CODE);
	}
	
	@Override
	public long getDownloadSize() {
		return row.getLong(CassandraProperties.FIELD_CRAWL_LOG_DOWNLOADED_BYTES);
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
		return row.getString(CassandraProperties.FIELD_CRAWL_LOG_DISCOVERY_PATH);
	}

	@Override
	public String getReferrer() {
		return row.getString(CassandraProperties.FIELD_CRAWL_LOG_REFERER);
	}

	@Override
	public String getContentType() {
		return row.getString(CassandraProperties.FIELD_CRAWL_LOG_CONTENT_TYPE);
	}

	@Override
	public String getWorkerThread() {
		return row.getString(CassandraProperties.FIELD_CRAWL_LOG_WORKER_THREAD);
	}
	
	@Override
	public Date getFetchTimestamp() {
		return new Date(row.getLong(CassandraProperties.FIELD_CRAWL_LOG_FETCH_TS));
	}

	@Override
	public int getFetchDuration() {
		// return row.getInt("fetch_duration");
		return 0;
	}
	
	@Override
	public String getSHA1Hash() {
		return row.getString(CassandraProperties.FIELD_CRAWL_LOG_HASH);
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
