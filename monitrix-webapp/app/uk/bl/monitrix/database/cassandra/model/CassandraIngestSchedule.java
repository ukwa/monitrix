package uk.bl.monitrix.database.cassandra.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import play.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.IngestSchedule;
import uk.bl.monitrix.model.IngestedLog;

/**
 * A CassandraDB-backed implementation of {@link IngestSchedule}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraIngestSchedule extends IngestSchedule {
	
	protected Session session; 

	private PreparedStatement statement = null;
	
	public CassandraIngestSchedule(Session session) {
		this.session = session;
		this.statement = session.prepare(
			      "INSERT INTO crawl_uris.log_files " +
					      "(path, crawler_id, is_monitored) " +
					      "VALUES (?, ?, ?);");
	}

	@Override
	public IngestedLog addLog(String path, String crawlerId, boolean monitor) {
		// Check if this log is already in the DB (path and crawlerID must be unique):
		if( getLogForCrawlerId(crawlerId) != null ) return null;
		if( getLogForPath(path) != null ) return null;
		// Add the log:
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(
				path,
				crawlerId,
				monitor
				));
		return new CassandraIngestedLog(
				session.execute("SELECT * FROM crawl_uris.log_files WHERE path='"+path+"'").one(),
				session.execute("SELECT * FROM crawl_uris.log_file_counters WHERE path='"+path+"'").one()
				);
	}
	
	@Override
	public List<IngestedLog> getLogs() {
		List<IngestedLog> logs = new ArrayList<IngestedLog>();
		Iterator<Row> cursor = session.execute("SELECT * FROM crawl_uris.log_files;").iterator();
		while(cursor.hasNext()) {
			Row r = cursor.next();
			ResultSet totals = session.execute("SELECT * FROM crawl_uris.log_file_counters WHERE path='"+r.getString("path")+"';");
			Row t = totals.one();
			CassandraIngestedLog cil = new CassandraIngestedLog(r,t);
			logs.add(cil);
		}
		return logs;
	}
	
	@Override
	public IngestedLog getLog(String id) {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.log_files WHERE path='"+id+"';");
		if (results.isExhausted()) return null;
		
		Row r = results.one();
		
		ResultSet totals = session.execute("SELECT * FROM crawl_uris.log_file_counters WHERE path='"+r.getString("path")+"';");		
		
		return new CassandraIngestedLog(r, totals.one());
	}
	
	@Override
	public IngestedLog getLogForCrawlerId(String crawlerId) {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.log_files WHERE crawler_id='"+crawlerId+"';");
		if (results.isExhausted()) return null;
		
		Row r = results.one();
		ResultSet totals = session.execute("SELECT * FROM crawl_uris.log_file_counters WHERE path='"+r.getString("path")+"';");
		
		return new CassandraIngestedLog(r, totals.one());
	}
	
	@Override
	public IngestedLog getLogForPath(String path) {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.log_files WHERE path='"+path+"';");
		if (results.isExhausted()) return null;

		ResultSet totals = session.execute("SELECT * FROM crawl_uris.log_file_counters WHERE path='"+path+"';");

		return new CassandraIngestedLog(results.one(), totals.one());
	}
	
	@Override
	public boolean isMonitoringEnabled(String id) {
		IngestedLog log = getLog(id);
		if (log == null)
			return false;
		
		return log.isMonitored();
	}

	@Override
	public void setMonitoringEnabled(String id, boolean monitoringEnabled) {
		CassandraIngestedLog log = (CassandraIngestedLog) getLog(id);
		if (log != null) {
			session.execute("UPDATE crawl_uris.log_files SET is_monitored = "+monitoringEnabled+" WHERE path='"+id+"'");
		}
	}
	
	public void incrementIngestedLogLines(String id, long increment, long revist_increment) {
		session.execute("UPDATE crawl_uris.log_file_counters SET ingested_lines = ingested_lines + "+increment
				+", revisit_records = revisit_records + "+revist_increment+" WHERE path='"+id+"'");
	}

}
