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
	// private PreparedStatement ingestScheduleStatement = null;
	
	public CassandraCrawlLogImporter(Session db) {
		super(db);
		
		this.crawlLogStatement = session.prepare(
				"INSERT INTO crawl_uris.crawl_log (" +
			    "log_id, timestamp, long_timestamp, coarse_timestamp, status_code, downloaded_bytes, uri, " + 
			    "discovery_path, referer, content_type, worker_thread, fetch_ts, hash, annotations, ip_address, " + 
			    "line) " +
			    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
		
//		this.ingestScheduleStatement = session.prepare(
//			    "INSERT INTO crawl_uris.ingest_schedule " +
//			    "(crawl_id, log_path, start_ts, end_ts, ingested_lines, revisit_records, is_monitored) " +
//				"VALUES (?, ?, ?, ?, ?, ?, ?);");		
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
			l.getBreadcrumbCodes(),
			l.getReferrer(),
			l.getContentType(),
			l.getWorkerThread(),
			fetch_ts.getTime(),
			l.getSHA1Hash(),
			l.getAnnotations(),
			"192.0.0.1",
			l.toString()));
		
		/* Also insert into URI table, for look-up purposes:
		BoundStatement boundStatementUri = new BoundStatement(statementUri);
		session.execute(boundStatementUri.bind(
				l.getURL(),
				log_ts,
				coarse_ts,
				fetch_ts,
				l.getHTTPCode(),
				l.getSHA1Hash()
				)); */
		
		/* Also stow annotations in a separate table, allowing annotation-based lookup.
		BoundStatement boundStatementAnno = new BoundStatement(statementAnno);
		for( String anno : l.getAnnotations().split(",") ) {
			if( anno.startsWith(CrawlLogEntry.ANNOTATION_CAPPED_CRAWL)) {
			session.execute(boundStatementAnno.bind(
					anno,
					l.getURL(),
					l.getLogTimestamp(),
					l.getHost()
					));
			}
		} */
		// FIXME Also increment url-level counters?
	}
	
	/**
	 * {@see http://wiki.apache.org/cassandra/FAQ#working_with_timeuuid_in_java}
	 * 
	 * These collide.
	 * 
	 * @param d
	 * @return
	 *
	public static java.util.UUID uuidForDate(Date d) {
		// Magic number obtained from #cassandra's thobbs, who
		// claims to have stolen it from a Python library.

        final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

        long origTime = d.getTime();
        long time = origTime * 10000 + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;
        long timeLow = time &       0xffffffffL;
        long timeMid = time &   0xffff00000000L;
        long timeHi = time & 0xfff000000000000L;
        long upperLong = (timeLow << 32) | (timeMid >> 16) | (1 << 12) | (timeHi >> 48) ;
        return new java.util.UUID(upperLong, 0xC000000000000000L);
    }
	
	public void insert(final List<LogFileEntry> log) {
		for(LogFileEntry log_entry : log ) {
			this.insert(log_entry);
		}
	}
	*/
	
}
