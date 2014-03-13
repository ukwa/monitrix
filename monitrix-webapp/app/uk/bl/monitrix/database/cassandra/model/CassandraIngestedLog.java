package uk.bl.monitrix.database.cassandra.model;


import com.datastax.driver.core.Row;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.IngestedLog;

public class CassandraIngestedLog implements IngestedLog {
	
	private Row row;
	
	public CassandraIngestedLog(Row row) {
		this.row = row;
	}
	
	@Override
	public String getId() {
		return row.getString(CassandraProperties.FIELD_INGEST_CRAWL_ID);
	}

	@Override
	public String getPath() {
		return row.getString(CassandraProperties.FIELD_INGEST_CRAWLER_PATH);
	}
	
	@Override
	public String getCrawlerId() {
		return row.getString(CassandraProperties.FIELD_INGEST_CRAWL_ID);
	}
	
	@Override
	public boolean isMonitored() {
		return row.getBool(CassandraProperties.FIELD_INGEST_IS_MONITORED);
	}
	
	@Override
	public long getIngestedLines() {
		return row.getLong(CassandraProperties.FIELD_INGEST_INGESTED_LINES);
	}
	
}
