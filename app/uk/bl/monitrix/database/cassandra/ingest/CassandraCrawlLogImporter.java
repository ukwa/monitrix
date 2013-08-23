package uk.bl.monitrix.database.cassandra.ingest;

import java.util.Date;
import java.util.List;

import play.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraDBConnector;
import uk.bl.monitrix.database.cassandra.model.CassandraCrawlLog;
import uk.bl.monitrix.heritrix.LogFileEntry;

/**
 * An extended version of {@link CassandraCrawlLog} that adds insert capability.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class CassandraCrawlLogImporter extends CassandraCrawlLog {

	private PreparedStatement statement = null;
	private PreparedStatement statementUri = null;
	private PreparedStatement statementCrawls = null;
	
	public CassandraCrawlLogImporter(Session db) {
		super(db);
		this.statement = session.prepare(
			      "INSERT INTO crawl_uris.log " +
			      "(coarse_ts, log_ts, uri, fetch_ts, host, domain, subdomain, status_code, hash, " + 
			      "log_id, annotations, discovery_path, compressibility, content_type, download_size, " + 
			      "fetch_duration, referer, retries, worker_thread) " +
			      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
		this.statementUri = session.prepare(
			      "INSERT INTO crawl_uris.uris " +
			      "(uri, log_ts, coarse_ts, fetch_ts, status_code, hash) " +
			      "VALUES (?, ?, ?, ?, ?, ?);");
		this.statementCrawls = session.prepare(
			      "INSERT INTO crawl_uris.crawls " +
			      "(crawl_id, start_ts, end_ts, profile) " +
			      "VALUES (?, ?, ?, ?);");
	}
	
	private void addCrawlInfo(String crawl_id, long start_ts, long end_ts ) {
		// Otherwise, insert:
		BoundStatement boundStatement = new BoundStatement(statementCrawls);
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
		
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(
				coarse_ts,
				log_ts,
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
				l.getWorkerThread()
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
	}
	
	public void insert(final List<LogFileEntry> log) {
		for(LogFileEntry log_entry : log ) {
			this.insert(log_entry);
		}
	}
	
}
