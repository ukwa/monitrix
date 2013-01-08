package uk.bl.monitrix.db.mongodb.model;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

import uk.bl.monitrix.db.mongodb.MongoProperties;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class AlertsCollection {
	
	private DBCollection collection;
	
	public AlertsCollection(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_ALERTS);
	}
	
	public void insert(AlertsDBO alert) {
		collection.insert(alert.dbo);
	}
	
	public List<String> getOffendingHosts() {
		@SuppressWarnings("rawtypes")
		final List offendingHosts = collection.distinct(MongoProperties.FIELD_ALERTS_OFFENDING_HOST);
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
	
	public long countAlertsForHost(String hostname) {
		return collection.count(new BasicDBObject(MongoProperties.FIELD_ALERTS_OFFENDING_HOST, hostname));
	}
	
	public Iterator<AlertsDBO> getAlertsForHost(String hostname) {
		return map(collection.find(new BasicDBObject(MongoProperties.FIELD_ALERTS_OFFENDING_HOST, hostname)));
	}
	
	public Iterator<AlertsDBO> listAll() {
		return map(collection.find());
	}
	
	public long countAll() {
		return collection.count();
	}
	
	private Iterator<AlertsDBO> map(final DBCursor cursor) {
		return new Iterator<AlertsDBO>() {		
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public AlertsDBO next() {
				return new AlertsDBO(cursor.next());
			}

			@Override
			public void remove() {
				cursor.remove();
			}
		};
	}

}
