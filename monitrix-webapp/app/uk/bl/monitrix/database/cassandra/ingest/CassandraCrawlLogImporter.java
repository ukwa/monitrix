package uk.bl.monitrix.database.cassandra.ingest;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.model.CassandraCrawlLog;
import uk.bl.monitrix.heritrix.LogFileEntry;
import uk.bl.monitrix.model.CrawlLogEntry;

/**
 * An extended version of {@link CassandraCrawlLog} that adds insert capability.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class CassandraCrawlLogImporter extends CassandraCrawlLog {

	private PreparedStatement crawlLogStatement = null;
	private PreparedStatement statementUri = null;
	private PreparedStatement ingestScheduleStatement = null;
	private PreparedStatement statementAnno = null;
	
	public CassandraCrawlLogImporter(Session db) {
		super(db);
		
		this.crawlLogStatement = session.prepare(
				"INSERT INTO crawl_uris.crawl_log (" +
			    "log_id, timestamp, long_timestamp, coarse_timestamp, status_code, downloaded_bytes, uri, " + 
			    "discovery_path, referer, content_type, worker_thread, fetch_ts, hash, annotations, ip_address, " + 
			    "line) " +
			    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
		
		this.ingestScheduleStatement = session.prepare(
			    "INSERT INTO crawl_uris.ingest_schedule " +
			    "(crawl_id, log_path, start_ts, end_ts, ingested_lines, revisit_records, is_monitored) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?);");		
	}
	
	private void addCrawlInfo(String crawl_id, long start_ts, long end_ts ) {
		// Otherwise, insert:
		BoundStatement boundStatement = new BoundStatement(ingestScheduleStatement);
		session.execute(boundStatement.bind(
				crawl_id,
				new Date(start_ts),
				new Date(end_ts),
				"no-profile"
				));		
	}
	
	public void updateCrawlInfo(String crawl_id, long timeOfFirstLogEntryInPatch, long timeOfLastLogEntryInPatch ) {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.crawls WHERE crawl_id='"+crawl_id+"';");
		// Don't do it if that crawl-id is already known:
		if( results.isExhausted() ) {
			this.addCrawlInfo(crawl_id, timeOfFirstLogEntryInPatch, timeOfLastLogEntryInPatch);
			return;
		}
		Row r = results.one();
		long start_ts = r.getDate("start_ts").getTime();
		if( timeOfFirstLogEntryInPatch < start_ts ) start_ts = timeOfFirstLogEntryInPatch;
		long end_ts = r.getDate("end_ts").getTime();
		if( timeOfLastLogEntryInPatch > end_ts ) end_ts = timeOfLastLogEntryInPatch;
		// Update the timestamps, as required:
		session.execute("UPDATE crawl_uris.crawls SET start_ts='"+start_ts+"', end_ts='"+end_ts+"' WHERE crawl_id='"+crawl_id+"';");
	}
	
	public void insert(LogFileEntry l) {
		// Check timestamp - should be the discovery/queue timestamp:
		Date log_ts = l.getLogTimestamp();
		Date fetch_ts = l.getFetchTimestamp();
		if( fetch_ts == null ) {
			fetch_ts = log_ts;
		}
		Date coarse_ts = this.getCoarseTimestamp(log_ts);
		
		BoundStatement boundStatement = new BoundStatement(crawlLogStatement);
		session.execute(boundStatement.bind(
				coarse_ts,
				log_ts,
				UUID.randomUUID(),
				l.getURL(),
				fetch_ts,
				l.getHost(),
				l.getDomain(),
				l.getSubdomain(),
				l.getHTTPCode(),
				l.getSHA1Hash(),
				l.getLogId(),
				l.getAnnotations(),
				l.getBreadcrumbCodes(),
				l.getCompressability(),
				l.getContentType(),
				l.getDownloadSize(),
				l.getFetchDuration(),
				l.getReferrer(),
				l.getRetries(),
				l.getWorkerThread(),
				l.toString()
				));
		// Also insert into URI table, for look-up purposes:
		BoundStatement boundStatementUri = new BoundStatement(statementUri);
		session.execute(boundStatementUri.bind(
				l.getURL(),
				log_ts,
				coarse_ts,
				fetch_ts,
				l.getHTTPCode(),
				l.getSHA1Hash()
				));
		// Also stow annotations in a separate table, allowing annotation-based lookup.
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
		}
		// FIXME Also increment url-level counters?
	}
	
	/**
	 * {@see http://wiki.apache.org/cassandra/FAQ#working_with_timeuuid_in_java}
	 * 
	 * These collide.
	 * 
	 * @param d
	 * @return
	 */
	public static java.util.UUID uuidForDate(Date d)
    {
/*
Magic number obtained from #cassandra's thobbs, who
claims to have stolen it from a Python library.
*/
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
	
}
