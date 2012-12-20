package uk.bl.monitrix.util;

import java.io.IOException;

import uk.bl.monitrix.db.mongodb.MongoConnector;
import uk.bl.monitrix.heritrix.LogfileReader;

/**
 * A utility class that pre-initializes MongoDB from a Heretrix log - for test/dev purposes only!
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class LogProcessor {
	
	// private static final String LOG_FILE = "/home/simonr/Downloads/sample-log-2E6.log";
	private static final String LOG_FILE = "/home/simonr/Downloads/crawl.log.20120914182409";
	
	public static void main(String[] args) throws IOException {
		MongoConnector mongo = new MongoConnector("localhost", "monitrix", 27017);
		LogfileReader reader = new LogfileReader(LOG_FILE);
		mongo.insert(reader.iterator());
	}

}
