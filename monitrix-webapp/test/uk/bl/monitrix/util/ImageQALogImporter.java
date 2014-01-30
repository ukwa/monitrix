package uk.bl.monitrix.util;

import java.io.IOException;

import com.mongodb.DB;
import com.mongodb.Mongo;

import uk.bl.monitrix.extensions.imageqa.csv.CsvImageQALog;
import uk.bl.monitrix.extensions.imageqa.mongodb.ingest.MongoImageQALogImporter;

public class ImageQALogImporter {
	
	private static final String IMAGE_QA_LOG_PATH = "test/image-qa.txt";
	
	public static void main(String[] args) throws IOException {
		CsvImageQALog csv = new CsvImageQALog(IMAGE_QA_LOG_PATH);
		
		Mongo mongo = new Mongo("localhost", 27017);
		DB db = mongo.getDB("monitrix");
		
		MongoImageQALogImporter importer = new MongoImageQALogImporter(db);
		importer.insert(csv.iterator());
	}

}
