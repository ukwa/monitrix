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
			dbo.setTimestamp(entry.getTimestamp());
			dbo.setExecutionTime(entry.getExecutionTime());
			dbo.setOriginalWebURL(entry.getOriginalWebURL());
			dbo.setWaybackImageURL(entry.getWaybackImageURL());
			dbo.setWaybackTimestamp(entry.getWaybackTimestamp());
			dbo.setFC1(entry.getFC1());
			dbo.setFC2(entry.getFC2());
			dbo.setMC(entry.getMC());
			dbo.setMessage(entry.getMessage());
			dbo.setTS1(entry.getTS1());
			dbo.setTS2(entry.getTS2());
			dbo.setOCR(entry.getOCR());
			dbo.setImage1Size(entry.getImage1Size());
			dbo.setImage2Size(entry.getImage2Size());
			dbo.setPSNRSimilarity(entry.getPSNRSimilarity());
			dbo.setPSNRThreshold(entry.getPSNRThreshold());
			dbo.setPSNRMessage(entry.getPSNRMessage());			
			dbo.setOriginalImageURL(entry.getOriginalImageURL());
			
			Logger.debug("Inserting image QA log entry: " + entry.toString());
			collection.insert(dbo.getBackingDBO());
		}
	}

}
