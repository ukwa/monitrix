package uk.bl.monitrix.heritrix;

import java.util.Iterator;

import uk.bl.monitrix.database.DBBatchImporter;
import uk.bl.monitrix.heritrix.LogFileEntry;

public class DummyBatchImporter implements DBBatchImporter {

	@Override
	public long countEntriesForCrawler(String logPath) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public void insert(String logPath, Iterator<LogFileEntry> iterator) {
		while (iterator.hasNext())
			iterator.next();
	}

}
