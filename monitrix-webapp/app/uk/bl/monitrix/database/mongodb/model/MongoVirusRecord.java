package uk.bl.monitrix.database.mongodb.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.VirusRecord;

public class MongoVirusRecord implements VirusRecord {
	
	private DBObject dbo;
	
	public MongoVirusRecord(DBObject dbo) {
		this.dbo = dbo;
	}
	
	/**
	 * Returns the MongoDB entity that's backing this object.
	 * @return the DBObject
	 */
	public DBObject getBackingDBO() {
		return dbo;
	}

	@Override
	public String getName() {
		return (String) dbo.get(MongoProperties.FIELD_VIRUS_LOG_NAME);
	}
	
	public void setName(String name) {
		dbo.put(MongoProperties.FIELD_VIRUS_LOG_NAME, name);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Integer> getOccurences() {
		DBObject occurences = (DBObject) dbo.get(MongoProperties.FIELD_VIRUS_LOG_OCCURENCES);
		if (occurences == null)
			return new HashMap<String, Integer>();
		
		Map<String, Integer> unescaped = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : ((Map<String, Integer>) occurences.toMap()).entrySet()) {
			unescaped.put(entry.getKey().replace("@@@", "."), entry.getValue());			
		}
		
		return unescaped;
	}
	
	public void setOccurences(Map<String, Integer> occurences) {
		Map<String, Integer> escaped = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : occurences.entrySet()) {
			escaped.put(entry.getKey().replace(".", "@@@"), entry.getValue());
		}
		
		dbo.put(MongoProperties.FIELD_VIRUS_LOG_OCCURENCES, new BasicDBObject(escaped));
	}

}
