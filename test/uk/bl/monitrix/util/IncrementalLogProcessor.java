package uk.bl.monitrix.util;

import java.io.IOException;

import uk.bl.monitrix.database.mongodb.ingest.MongoBatchImporter;
import uk.bl.monitrix.heritrix.IncrementalLogfileReader;

public class IncrementalLogProcessor {
	
	private static final String LOG_FILE = "/home/simonr/dummy.log";
	
	public static void main(String[] args) throws IOException, InterruptedException {
		MongoBatchImporter mongo = new MongoBatchImporter("localhost", "monitrix", 27017);
		IncrementalLogfileReader reader = new IncrementalLogfileReader(LOG_FILE);
		
		while (true) {
			System.out.println("Loading next batch");
			mongo.insert(reader.newIterator());
			System.out.println("Done - waiting 30s");
			Thread.sleep(30000);
		}
	}

}
