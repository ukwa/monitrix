package uk.bl.monitrix.database.cassandra.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Row;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.VirusRecord;

public class CassandraVirusRecord implements VirusRecord {
	
	private String virusName;
	
	private Map<String, Integer> occurrences;
	
	@SuppressWarnings("unchecked")
	public CassandraVirusRecord(Row row) {
		this.virusName = row.getString(CassandraProperties.FIELD_VIRUS_LOG_NAME);
		
		try {
			this.occurrences = new ObjectMapper().readValue(row.getString(CassandraProperties.FIELD_VIRUS_LOG_OCCURENCES), HashMap.class);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String getName() {
		return virusName;
	}
	
	@Override
	public Map<String, Integer> getOccurences() {
		return occurrences;
	}
	
}
