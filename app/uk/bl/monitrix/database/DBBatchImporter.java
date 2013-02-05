package uk.bl.monitrix.database;

import java.util.Iterator;

import uk.bl.monitrix.heritrix.LogFileEntry;

public interface DBBatchImporter {
	
	public void insert(Iterator<LogFileEntry> iterator);

}
