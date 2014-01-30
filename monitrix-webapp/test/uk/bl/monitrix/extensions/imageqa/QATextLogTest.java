package uk.bl.monitrix.extensions.imageqa;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

import uk.bl.monitrix.extensions.imageqa.csv.CsvImageQALog;
import uk.bl.monitrix.extensions.imageqa.model.ImageQALogEntry;

public class QATextLogTest {
	
	private static final String LOG_PATH = "test/image-qa.txt";
	
	@Test
	public void testLogRead() throws IOException {
		CsvImageQALog log = new CsvImageQALog(LOG_PATH);
		Iterator<ImageQALogEntry> it = log.iterator();
		while(it.hasNext())
			System.out.println(it.next());
	}

}
