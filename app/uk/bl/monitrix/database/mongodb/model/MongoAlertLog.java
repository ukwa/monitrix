package uk.bl.monitrix.database.mongodb.model;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.model.Alert;
import uk.bl.monitrix.model.AlertLog;

/**
 * A MongoDB-backed implementation of {@link AlertLog}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 *
 */
public class MongoAlertLog implements AlertLog {
	
	private DBCollection collection;
	
	public MongoAlertLog(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_ALERT_LOG);
	}

	@Override
	public long countAll() {
		return collection.count();
	}

	@Override
	public Iterator<Alert> listAll() {
		return map(collection.find());
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
	public long countForHost(String hostname) {
		return collection.count(new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_OFFENDING_HOST, hostname));
	}

	@Override
	public Iterator<Alert> listForHost(String hostname) {
		return map(collection.find(new BasicDBObject(MongoProperties.FIELD_ALERT_LOG_OFFENDING_HOST, hostname)));
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
