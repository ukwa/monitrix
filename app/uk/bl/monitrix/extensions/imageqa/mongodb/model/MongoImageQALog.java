package uk.bl.monitrix.extensions.imageqa.mongodb.model;

import java.util.Iterator;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.extensions.imageqa.model.ImageQALog;
import uk.bl.monitrix.extensions.imageqa.model.ImageQALogEntry;

public class MongoImageQALog implements ImageQALog {
	
	protected DBCollection collection;
	
	public MongoImageQALog(DB db) {
		this.collection = db.getCollection(MongoProperties.COLLECTION_ALERT_LOG);
	}

	@Override
	public long countEntries() {
		return collection.count();
	}

	@Override
	public Iterator<ImageQALogEntry> listEntries() {
		final DBCursor cursor = collection.find();
		
		return new Iterator<ImageQALogEntry>() {
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public ImageQALogEntry next() {
				// TODO implement
				return null;
			}

			@Override
			public void remove() {
				cursor.remove();
			}
		};
	}

}
