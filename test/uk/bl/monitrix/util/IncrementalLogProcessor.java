package uk.bl.monitrix.util;

import java.io.IOException;

import uk.bl.monitrix.database.mongodb.ingest.MongoDBIngestConnector;
import uk.bl.monitrix.heritrix.IncrementalLogfileReader;

public class IncrementalLogProcessor {
	
	private static final String LOG_FILE = "/home/simonr/dummy.log";
	
	public static void main(String[] args) throws IOException, InterruptedException {
		MongoDBIngestConnector mongo = new MongoDBIngestConnector("localhost", "monitrix", 27017);
		IncrementalLogfileReader reader = new IncrementalLogfileReader(LOG_FILE);
		
		while (true) {
			System.out.println("Loading next batch");
			mongo.insert(LOG_FILE, reader.newIterator());
			System.out.println("Done - waiting 15s");
			Thread.sleep(15000);
		}
	}

}
