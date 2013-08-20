package uk.bl.monitrix.database.cassandra.ingest;

import java.util.AbstractList;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBObject;

import uk.bl.monitrix.database.mongodb.model.MongoCrawlLog;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlLogEntry;

/**
 * An extended version of {@link MongoCrawlLog} that adds insert capability.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class CassandraCrawlLogImporter extends MongoCrawlLog {

	public CassandraCrawlLogImporter(DB db) {
		super(db);
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
