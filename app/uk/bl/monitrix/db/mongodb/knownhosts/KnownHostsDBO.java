package uk.bl.monitrix.db.mongodb.knownhosts;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.DBObject;

public class KnownHostsDBO {
	
	DBObject dbo;
	
	public KnownHostsDBO(DBObject dbo) {
		this.dbo = dbo;
	}
	
	public String getHostname() {
		return (String) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME);
	}
	
	public void setHostname(String hostname) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, hostname);
	}
	
	public long getLastAccess() {
		return (Long) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS);
	}
	
	public void setLastAccess(long lastAccess) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS, lastAccess);
	}

}
