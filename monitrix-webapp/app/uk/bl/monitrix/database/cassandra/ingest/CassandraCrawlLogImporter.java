package uk.bl.monitrix.database.cassandra.ingest;

import java.util.Date;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.database.cassandra.model.CassandraCrawlLog;
import uk.bl.monitrix.heritrix.LogFileEntry;

/**
 * An extended version of {@link CassandraCrawlLog} that adds insert capability.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class CassandraCrawlLogImporter extends CassandraCrawlLog {
	
	private static final String TABLE_INGEST_SCHEDULE = CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_INGEST_SCHEDULE;

	private PreparedStatement crawlLogStatement = null;
	
	public CassandraCrawlLogImporter(Session db) {
		super(db);
		
		this.crawlLogStatement = session.prepare(
				"INSERT INTO crawl_uris.crawl_log (" +
			    "log_id, timestamp, long_timestamp, coarse_timestamp, status_code, downloaded_bytes, uri, host, " + 
			    "discovery_path, referer, content_type, worker_thread, fetch_ts, hash, annotations, ip_address, " + 
			    "compressability, line) " +
			    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");	
	}
	
	public void updateCrawlInfo(String crawl_id, long timeOfFirstLogEntryInPatch, long timeOfLastLogEntryInPatch ) {
		ResultSet results = 
				session.execute("SELECT * FROM " + TABLE_INGEST_SCHEDULE + " WHERE " + CassandraProperties.FIELD_INGEST_CRAWL_ID + "='" + crawl_id + "';");
		
		Row r = results.one();
		
		long startTs = r.getLong(CassandraProperties.FIELD_INGEST_START_TS);
		long endTs = r.getLong(CassandraProperties.FIELD_INGEST_END_TS);
		
		if (startTs == 0 || timeOfFirstLogEntryInPatch < startTs)
			startTs = timeOfFirstLogEntryInPatch;
		
		if (timeOfLastLogEntryInPatch > endTs )
			endTs = timeOfLastLogEntryInPatch;
		
		session.execute("UPDATE " + TABLE_INGEST_SCHEDULE + " SET " + CassandraProperties.FIELD_INGEST_START_TS + "=" + startTs +
			", " + CassandraProperties.FIELD_INGEST_END_TS + "=" + endTs + " WHERE " + CassandraProperties.FIELD_INGEST_CRAWL_ID + "='" + crawl_id + "';");			
	}
	
	public void insert(LogFileEntry l) {
		// Check timestamp - should be the discovery/queue timestamp:
		Date log_ts = l.getLogTimestamp();
		Date fetch_ts = l.getFetchTimestamp();
		if (fetch_ts == null)
			fetch_ts = log_ts;

		Date coarse_ts = getCoarseTimestamp(log_ts);
		
		BoundStatement boundStatement = new BoundStatement(crawlLogStatement);
		session.execute(boundStatement.bind(
			l.getLogId(),
			log_ts.toString(),
			log_ts.getTime(),
			coarse_ts.getTime(),
			l.getHTTPCode(),
			l.getDownloadSize(),
			l.getURL(),
			l.getHost(),
			l.getBreadcrumbCodes(),
			l.getReferrer(),
			l.getContentType(),
			l.getWorkerThread(),
			fetch_ts.getTime(),
			l.getSHA1Hash(),
			l.getAnnotations(),
			l.getAnnotations(),
			l.getCompressability(),
			l.toString()));
	}
	
}
