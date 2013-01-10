package uk.bl.monitrix.heritrix;

import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Test;

public class LogReaderTest {
	
	/** a tiny, 1000-line log file for unit testing **/
	private static final String PATH_TO_LOGFILE = "test/sample-log-1E3.txt";

	@Test
	public void testLogRead() throws IOException {
		LogfileReader reader = new LogfileReader(PATH_TO_LOGFILE);
		Iterator<LogFileEntry> entries = reader.iterator();
		
		int counter = 0;
		while (entries.hasNext()) {
			LogFileEntry entry = entries.next();
			Assert.assertTrue(entry.getTimestamp().getTime() > 0);
			Assert.assertNotNull(entry.getHTTPCode()); // Just make sure the method gets called
			Assert.assertTrue(entry.getDownloadSize() > -1);
			Assert.assertFalse(entry.getURL().isEmpty());
			Assert.assertNotNull(entry.getContentType());
			Assert.assertNotNull(entry.getCrawlerID());
			Assert.assertNotNull(entry.getSHA1Hash());
			counter++;
		}
		
		Assert.assertEquals(1000, counter);
		System.out.println("Done - " + counter + " lines.");
	}
	
}
