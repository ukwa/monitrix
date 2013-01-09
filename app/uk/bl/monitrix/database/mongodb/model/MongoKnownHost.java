package uk.bl.monitrix.database.mongodb.model;

import java.util.Arrays;

import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.KnownHost;

/**
 * A MongoDB-backed implementation of {@link KnownHost}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 *
 */
public class MongoKnownHost extends KnownHost {
	
	private DBObject dbo;
	
	public MongoKnownHost(DBObject dbo) {
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
	public String getHostname() {
		return dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME).toString();
	}
	
	public void setHostname(String hostname) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME, hostname);
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_HOSTNAME_TOKENIZED,
				Arrays.asList(KnownHost.tokenizeName(hostname)));
	}

	@Override
	public long getFirstAccess() {
		return (Long) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_FIRST_ACCESS);
	}
	
	public void setFirstAccess(long firstAccess) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_FIRST_ACCESS, firstAccess);
	}

	@Override
	public long getLastAccess() {
		return (Long) dbo.get(MongoProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS);
	}
	
	public void setLastAccess(long lastAccess) {
		dbo.put(MongoProperties.FIELD_KNOWN_HOSTS_LAST_ACCESS, lastAccess);
	}

}
