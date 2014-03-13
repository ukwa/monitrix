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

import uk.bl.monitrix.model.IngestSchedule;
import uk.bl.monitrix.model.IngestedLog;
import uk.bl.monitrix.database.cassandra.CassandraProperties;

/**
 * A CassandraDB-backed implementation of {@link IngestSchedule}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraIngestSchedule extends IngestSchedule {
	
	private final String TABLE_INGEST_SCHEDULE = CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_INGEST_SCHEDULE;
	
	protected Session session; 

	private PreparedStatement statement = null;
	
	public CassandraIngestSchedule(Session session) {
		this.session = session;
		this.statement = session.prepare(
				"INSERT INTO " + CassandraProperties.KEYSPACE + "." + CassandraProperties.COLLECTION_INGEST_SCHEDULE + " " +
				"(crawl_id, log_path, start_ts, end_ts, ingested_lines, revisit_records, is_monitored) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?);");
	}

	@Override
	public IngestedLog addLog(String path, String crawlerId, boolean monitor) {
		Logger.info("Adding log: " + crawlerId + " - " + path);
		
		if (getLogForCrawlerId(crawlerId) != null || getLogForPath(path) != null) {
			Logger.warn("Log is already in the DB!");
			return null;
		}
		
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(crawlerId, path, 0l, 0l, 0l, 0l, false));
		
		Row r = session.execute("SELECT * FROM " + TABLE_INGEST_SCHEDULE + " WHERE " + CassandraProperties.FIELD_INGEST_CRAWL_ID + "='" + crawlerId + "';").one();
		return new CassandraIngestedLog(r);
	}
	
	@Override
	public List<IngestedLog> getLogs() {
		Iterator<Row> cursor = session.execute("SELECT * FROM " + TABLE_INGEST_SCHEDULE + ";").iterator();

		List<IngestedLog> logs = new ArrayList<IngestedLog>();		
		while(cursor.hasNext()) {
			logs.add(new CassandraIngestedLog(cursor.next()));
		}
		
		return logs;
	}
	
	@Override
	public IngestedLog getLog(String id) {
		ResultSet results = 
				session.execute("SELECT * FROM " + TABLE_INGEST_SCHEDULE + " WHERE " + CassandraProperties.FIELD_INGEST_CRAWL_ID + "='" + id + "';");
		if (results.isExhausted())
			return null;
		
		return new CassandraIngestedLog(results.one());
	}
	
	@Override
	public IngestedLog getLogForCrawlerId(String crawlerId) {
		return getLog(crawlerId);
	}
	
	@Override
	public IngestedLog getLogForPath(String path) {
		ResultSet results = 
				session.execute("SELECT * FROM " + TABLE_INGEST_SCHEDULE + " WHERE " + CassandraProperties.FIELD_INGEST_CRAWLER_PATH  + "='" + path + "';");
		if (results.isExhausted())
			return null;

		return new CassandraIngestedLog(results.one());
	}
	
	@Override
	public boolean isMonitoringEnabled(String id) {
		IngestedLog log = getLog(id);
		if (log == null) {
			return false;
		}
		
		return log.isMonitored();
	}

	@Override
	public void setMonitoringEnabled(String id, boolean monitoringEnabled) {
		Logger.info("Setting monitoring to " + monitoringEnabled + " for log " + id);
		IngestedLog log = getLog(id);
		if (log != null)
			session.execute("UPDATE " + TABLE_INGEST_SCHEDULE + " SET " + CassandraProperties.FIELD_INGEST_IS_MONITORED + "=" + 
					monitoringEnabled + " WHERE " + CassandraProperties.FIELD_INGEST_CRAWL_ID + "='" + id + "';");
	}
	
	public void incrementIngestedLogLines(String id, long increment, long revist_increment) {
		IngestedLog log = getLog(id);
		if (log != null) {
			long ingestedLines = log.getIngestedLines() + increment;
			Logger.info("Setting ingested lines to " + ingestedLines+ " for log " + id);
			session.execute("UPDATE " + TABLE_INGEST_SCHEDULE + " SET " + CassandraProperties.FIELD_INGEST_INGESTED_LINES + "=" + 
					ingestedLines + " WHERE " + CassandraProperties.FIELD_INGEST_CRAWL_ID + "='" + id +"';");
		}
	}

}
