package uk.bl.monitrix.database.mongodb.ingest;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import play.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.model.MongoAlert;
import uk.bl.monitrix.database.mongodb.model.MongoKnownHost;
import uk.bl.monitrix.database.mongodb.model.MongoKnownHostList;
import uk.bl.monitrix.model.Alert.AlertType;

class MongoKnownHostImporter extends MongoKnownHostList {
	
	private static final String ALERT_MSG_TOO_MANY_SUBDOMAINS = "The host %s has a suspiciously high number of subdomains (%s)";
	
	private MongoAlertLogImporter alertLog;

	public MongoKnownHostImporter(DB db, MongoAlertLogImporter alertLog) {
		super(db);
		this.alertLog = alertLog;
	}
	
	/**
	 * Adds a new host to the Known Hosts list. Note that this method ONLY writes to
	 * the in-memory cache! In order to write to the database, execute the .commit() method
	 * after your additions are done.
	 * @param hostname the host name
	 * @param accessTime the access time
	 */
	public void addToList(String hostname, long accessTime) {	
		MongoKnownHost dbo = new MongoKnownHost(new BasicDBObject());
		dbo.setHostname(hostname);
		dbo.setFirstAccess(accessTime);
		dbo.setLastAccess(accessTime);
		cache.put(hostname, dbo);
	}
	
	/**
	 * Updates the last access time for the specified host. Note that this method ONLY 
	 * writes to the in-memory cache! In order to write to the database, execute the .commit()
	 * method after your additions are done.
	 * @param hostname the hostname
	 * @param lastAccess the new last access time
	 */
	public void setLastAccess(String hostname, long lastAccess) {		
		// In this case we know it's a safe cast
		MongoKnownHost dbo = (MongoKnownHost) getKnownHost(hostname);
		if (dbo != null)
			dbo.setLastAccess(lastAccess);
		else
			Logger.warn("Attempt to write last access info to unknown host: " + hostname);
	}
	
	/**
	 * Adds a subdomain to the specified host. Note that this method ONLY
	 * writes to the in-memory cache! In order to write to the database, execute the .commit()
	 * method after your additions are done.
	 * @param hostname the hostname
	 * @param subdomain the subdomain to add
	 */
	public void addSubdomain(String hostname, String subdomain) {
		// In this case we know it's a safe cast
		MongoKnownHost dbo = (MongoKnownHost) getKnownHost(hostname);
		if (dbo != null)
			dbo.addSubdomain(subdomain);
		else
			Logger.warn("Attempt to write subdomain info to unknown host: " + hostname);
	}
	
	/**
	 * Writes the contents of the cache to the database.
	 */
	public void commit() {
		// TODO risky - we'll lose updates to the database that haven't been made through this object instance!
		final List<MongoKnownHost> cachedKnownHosts = new ArrayList<MongoKnownHost>(cache.values());	
		collection.drop();
		collection.insert(new AbstractList<DBObject>() {
			@Override
			public DBObject get(int index) {
				return cachedKnownHosts.get(index).getBackingDBO();
			}

			@Override
			public int size() {
				return cachedKnownHosts.size();
			}	
		});
		
		// Compute host-level alerts
		// Note: we only need to consider hosts that were added in this batch - ie. those in the cache!
		for (MongoKnownHost host : cache.values()) {
			// Subdomain limit
			int subdomains = host.getSubdomains().size();
			if (subdomains > 100) {
				MongoAlert alert = new MongoAlert(new BasicDBObject());
				alert.setTimestamp(host.getLastAccess());
				alert.setOffendingHost(host.getHostname());
				alert.setAlertType(AlertType.TOO_MANY_SUBDOMAINS);
				alert.setAlertDescription(String.format(ALERT_MSG_TOO_MANY_SUBDOMAINS, host.getHostname(), Integer.toString(subdomains)));
				alertLog.insert(alert);
			}
		}
		
		cache.clear();
	}

}
