package uk.bl.monitrix.heritrix;

import java.util.Iterator;

import uk.bl.monitrix.database.DBBatchImporter;
import uk.bl.monitrix.heritrix.LogFileEntry;

public class DummyBatchImporter implements DBBatchImporter {

	@Override
	public void insert(Iterator<LogFileEntry> iterator) {
		while (iterator.hasNext())
			iterator.next();
	}

}
