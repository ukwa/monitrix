package uk.bl.monitrix.database.cassandra.model;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.Session;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.Alert;
import uk.bl.monitrix.model.Alert.AlertType;
import uk.bl.monitrix.model.AlertLog;

/**
 * A MongoDB-backed implementation of {@link AlertLog}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraAlertLog implements AlertLog {
	
	protected DBCollection collection;
	
	public CassandraAlertLog(Session session) {
		this.collection = session.getCollection(MongoProperties.COLLECTION_ALERT_LOG);
		
		// Collection is indexed by timestamp and host (will be skipped automatically if index exists)
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_TIMESTAMP, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_OFFENDING_HOST, 1));
	}

	@Override
	public long countAll() {
		return collection.count();
	}

	@Override
	public Iterator<Alert> listAll() {
		return map(collection.find().sort(new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_TIMESTAMP, -1)));
	}
	
	@Override
	public List<Alert> getMostRecent(int n) {
		DBCursor cursor = collection.find().sort(new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_TIMESTAMP, -1)).limit(n);
		
		List<Alert> recent = new ArrayList<Alert>();
		while(cursor.hasNext())
			recent.add(new MongoAlert(cursor.next()));

		return recent;
	}

	@Override
	public List<String> getOffendingHosts() {
		@SuppressWarnings("rawtypes")
		final List offendingHosts = collection.distinct(MongoProperties.FIELD_ALERT_LOG_OFFENDING_HOST);
		return new AbstractList<String>() {
			@Override
			public String get(int index) {
				return offendingHosts.get(index).toString();
			}

			@Override
			public int size() {
				return offendingHosts.size();
			}
		};
	}

	@Override
	public long countAlertsForHost(String hostname) {
		return collection.count(new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_OFFENDING_HOST, hostname));
	}
	
	@Override
	public long countAlertsForHost(String hostname, AlertType type) {
		DBObject query = new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_OFFENDING_HOST, hostname)
			.append(MongoProperties.FIELD_ALERT_LOG_ALERT_TYPE, type.name());
		
		return collection.count(query);
	}
	
	@Override
	public List<AlertType> getAlertTypesForHost(String hostname) {
		@SuppressWarnings("rawtypes")
		final List types = collection.distinct(MongoProperties.FIELD_ALERT_LOG_ALERT_TYPE, new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_OFFENDING_HOST, hostname));
				
		return new AbstractList<Alert.AlertType>() {
			@Override
			public AlertType get(int index) {
				return AlertType.valueOf(types.get(index).toString());
			}

			@Override
			public int size() {
				return types.size();
			}
		};
	}


	@Override
	public Iterator<Alert> listAlertsForHost(String hostname) {
		return map(collection.find(new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_OFFENDING_HOST, hostname)).sort(new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_TIMESTAMP, -1)));
	}
	
	/**
	 * Utility method that maps a database cursor to an Iterator of Alert domain objects. 
	 * @param cursor the DB cursor
	 * @return the domain objects
	 */
	private static Iterator<Alert> map(final DBCursor cursor) {
		return new Iterator<Alert>() {		
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public Alert next() {
				return new MongoAlert(cursor.next());
			}

			@Override
			public void remove() {
				cursor.remove();
			}
		};
	}
	
}
