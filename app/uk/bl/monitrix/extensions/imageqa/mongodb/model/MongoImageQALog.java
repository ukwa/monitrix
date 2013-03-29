package uk.bl.monitrix.extensions.imageqa.mongodb.model;

import java.util.Iterator;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import uk.bl.monitrix.database.ExtensionTable;
import uk.bl.monitrix.extensions.imageqa.model.ImageQALog;
import uk.bl.monitrix.extensions.imageqa.model.ImageQALogEntry;
import uk.bl.monitrix.extensions.imageqa.mongodb.MongoImageQAProperties;

public class MongoImageQALog extends ExtensionTable implements ImageQALog {
	
	protected DBCollection collection;
	
	public MongoImageQALog(DB db) {
		super(db);
		this.collection = db.getCollection(MongoImageQAProperties.COLLECTION_IMAGE_QA_LOG);
		this.collection.ensureIndex(new BasicDBObject(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_ORIGINAL_WEB_URL, 1));
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
				return new MongoImageQALogEntry(cursor.next());
			}

			@Override
			public void remove() {
				cursor.remove();
			}
		};
	}

	@Override
	public ImageQALogEntry findForURL(String url) {
		DBObject dbo = collection.findOne(new BasicDBObject(MongoImageQAProperties.FIELD_IMAGE_QA_LOG_ORIGINAL_IMAGE_URL, url));
		
		if (dbo == null)
			return null;
		
		return new MongoImageQALogEntry(dbo);
	}

}
