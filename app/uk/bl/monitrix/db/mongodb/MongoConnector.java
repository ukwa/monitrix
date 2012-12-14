package uk.bl.monitrix.db.mongodb;

import java.util.Iterator;

import uk.bl.monitrix.db.DBConnector;
import uk.bl.monitrix.heritrix.LogEntry;

/**
 * An implementation of {@link DBConnector} for MongoDB.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoConnector implements DBConnector {
	
	public MongoConnector() {
		
	}

	@Override
	public void insert(Iterator<LogEntry> iterator) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

}
