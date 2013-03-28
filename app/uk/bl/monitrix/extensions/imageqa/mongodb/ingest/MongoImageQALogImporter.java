package uk.bl.monitrix.extensions.imageqa.mongodb.ingest;

import java.util.Iterator;

import play.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;

import uk.bl.monitrix.extensions.imageqa.model.ImageQALogEntry;
import uk.bl.monitrix.extensions.imageqa.mongodb.model.MongoImageQALog;
import uk.bl.monitrix.extensions.imageqa.mongodb.model.MongoImageQALogEntry;

public class MongoImageQALogImporter extends MongoImageQALog {

	public MongoImageQALogImporter(DB db) {
		super(db);
	}
	
	public void insert(Iterator<ImageQALogEntry> entries) {
		// For now, we just insert everything one by one
		while (entries.hasNext()) {
			ImageQALogEntry entry = entries.next();
			
			MongoImageQALogEntry dbo = new MongoImageQALogEntry(new BasicDBObject());
			dbo.setOriginalWebURL(entry.getOriginalWebURL());
			dbo.setOriginalImageURL(entry.getOriginalImageURL());
			dbo.setWaybackImageURL(entry.getWaybackImageURL());
			dbo.setMessage(entry.getMessage());
			dbo.setPSNRMessage(entry.getPSNRMessage());
			dbo.setLogLine(entry.toString());
			
			Logger.debug("Inserting image QA log entry: " + entry.toString());
			collection.insert(dbo.getBackingDBO());
		}
	}

}
