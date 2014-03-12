package uk.bl.monitrix.database.cassandra.model;


import com.datastax.driver.core.Row;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.IngestedLog;

public class CassandraIngestedLog implements IngestedLog {
	
	private Row row;
	private Row rowTotal;
	
	public CassandraIngestedLog(Row row, Row total) {
		this.row = row;
		this.rowTotal = total;
	}
	
	@Override
	public String getId() {
		return row.getString(CassandraProperties.FIELD_INGEST_SCHEDULE_PATH);
	}

	@Override
	public String getPath() {
		return row.getString(CassandraProperties.FIELD_INGEST_SCHEDULE_PATH);
	}
	
	@Override
	public String getCrawlerId() {
		return row.getString(CassandraProperties.FIELD_INGEST_SCHEDULE_CRAWLER_ID);
	}
	
	@Override
	public boolean isMonitored() {
		return row.getBool(CassandraProperties.FIELD_INGEST_SCHEDULE_MONITORED);
	}
	
	@Override
	public long getIngestedLines() {
		if( rowTotal == null ) return 0L;
		return rowTotal.getLong(CassandraProperties.FIELD_INGEST_SCHEDULE_LINES);
	}
	
}
