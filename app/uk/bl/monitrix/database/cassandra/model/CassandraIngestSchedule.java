package uk.bl.monitrix.database.cassandra.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.types.ObjectId;

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

	PreparedStatement statement = session.prepare(
		      "INSERT INTO crawl_uris.log_files " +
		      "(path, crawler_id, monitor, ingested_lines) " +
		      "VALUES (?, ?, ?, ?);");

	
	public CassandraIngestSchedule(Session session) {
		this.session = session;
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
				monitor,
				0
				));
		return new CassandraIngestedLog(session.execute("SELECT * FROM crawl_uris.log_files WHERE path='"+path+"'").one());
	}
	
	@Override
	public List<IngestedLog> getLogs() {
		List<IngestedLog> logs = new ArrayList<IngestedLog>();
		Iterator<Row> cursor = session.execute("SELECT * FROM crawl_uris.log_files;").iterator();
		while(cursor.hasNext())
			logs.add(new CassandraIngestedLog(cursor.next()));
		return logs;
	}
	
	@Override
	public IngestedLog getLog(String id) {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.log_files WHERE path='"+id+"';");
		if (results.isExhausted())
			return null;
		
		return new CassandraIngestedLog(results.one());
	}
	
	@Override
	public IngestedLog getLogForCrawlerId(String crawlerId) {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.log_files WHERE crawler_id='"+crawlerId+"';");
		if (results.isExhausted())
			return null;
		
		return new CassandraIngestedLog(results.one());
	}
	
	@Override
	public IngestedLog getLogForPath(String path) {
		ResultSet results = session.execute("SELECT * FROM crawl_uris.log_files WHERE path='"+path+"';");
		if (results.isExhausted())
			return null;
		
		return new CassandraIngestedLog(results.one());
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
			session.execute("UPDATE crawl_uris.log_files SET monitor = "+monitoringEnabled+" WHERE path='"+id+"'");
		}
	}
	
	public void incrementIngestedLogLines(String id, long increment) {
		session.execute("UPDATE crawl_uris.log_files SET ingested_lines = ingested_lines + "+increment+" WHERE path='"+id+"'");
	}

}
