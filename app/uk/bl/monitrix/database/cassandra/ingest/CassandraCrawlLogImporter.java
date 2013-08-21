package uk.bl.monitrix.database.cassandra.ingest;

import java.util.Date;
import java.util.List;

import play.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraDBConnector;
import uk.bl.monitrix.database.cassandra.model.CassandraCrawlLog;
import uk.bl.monitrix.heritrix.LogFileEntry;

/**
 * An extended version of {@link CassandraCrawlLog} that adds insert capability.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class CassandraCrawlLogImporter extends CassandraCrawlLog {

	PreparedStatement statement = null;
	PreparedStatement statementUri = null;
	
	public CassandraCrawlLogImporter(Session db) {
		super(db);
		this.statement = session.prepare(
			      "INSERT INTO crawl_uris.log " +
			      "(coarse_ts, log_ts, uri, fetch_ts, host, domain, subdomain, status_code, hash, crawl_id, " + 
			      "annotations, discovery_path, compressibility, content_type, download_size, " + 
			      "fetch_duration, referrer, retries, worker_thread) " +
			      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
		this.statementUri = session.prepare(
			      "INSERT INTO crawl_uris.uris " +
			      "(uri, log_ts, coarse_ts, fetch_ts, status_code, hash) " +
			      "VALUES (?, ?, ?, ?, ?, ?);");
	}
	
	public void insert(LogFileEntry l) {
		// Check timestamp - should be the discovery/queue timestamp:
		Date log_ts = l.getLogTimestamp();
		Date fetch_ts = l.getFetchTimestamp();
		if( fetch_ts == null ) {
			fetch_ts = log_ts;
		}
		Date coarse_ts = new Date(CassandraDBConnector.HOUR_AS_MILLIS*(l.getLogTimestamp().getTime()/CassandraDBConnector.HOUR_AS_MILLIS));
		
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
