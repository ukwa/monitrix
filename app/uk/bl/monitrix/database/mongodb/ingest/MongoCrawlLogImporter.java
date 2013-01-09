package uk.bl.monitrix.database.mongodb.ingest;

import java.util.AbstractList;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;

import uk.bl.monitrix.database.mongodb.MongoProperties;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlLog;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlLogEntry;

public class MongoCrawlLogImporter extends MongoCrawlLog {

	public MongoCrawlLogImporter(DB db) {
		super(db);
		
		// The Heritrix Log collection is indexed by timestamp and hostname (will be skipped automatically if index exists)
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_TIMESTAMP, 1));
		this.collection.ensureIndex(new BasicDBObject(MongoProperties.FIELD_CRAWL_LOG_HOST, 1));
	}
	
	public void insert(final List<MongoCrawlLogEntry> log) {
		List<DBObject> mapped = new AbstractList<DBObject>() {
			@Override
			public DBObject get(int index) {
				return log.get(index).getBackingDBO();
			}

			@Override
			public int size() {
				return log.size();
			}
		};
		collection.insert(mapped);
	}
	
}
