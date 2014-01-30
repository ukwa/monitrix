package uk.bl.monitrix.database.cassandra.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.Row;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.VirusRecord;

public class CassandraVirusRecord implements VirusRecord {
	
	private Row row;
	
	public CassandraVirusRecord(Row row) {
		this.row = row;
	}
	
	@Override
	public String getName() {
		return row.getString(CassandraProperties.FIELD_VIRUS_LOG_NAME);
	}
	
	@Override
	public Map<String, Integer> getOccurences() {
		Map<String, Integer> occurences = row.getMap(CassandraProperties.FIELD_VIRUS_LOG_OCCURENCES, String.class, Integer.class);
		if (occurences == null)
			return new HashMap<String, Integer>();
		
		Map<String, Integer> unescaped = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : ((Map<String, Integer>) occurences).entrySet()) {
			unescaped.put(entry.getKey().replace("@@@", "."), entry.getValue());			
		}
		
		return unescaped;
	}
	
}
