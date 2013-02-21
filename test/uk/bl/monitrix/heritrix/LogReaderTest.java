package uk.bl.monitrix.heritrix;

import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Test;

public class LogReaderTest {
	
	/** a tiny, 1000-line log file for unit testing **/
	private static final String PATH_TO_LOGFILE = "test/sample-log-1E3.txt";

	@Test
	public void testSimpleLogReader() throws IOException {
		SimpleLogfileReader reader = new SimpleLogfileReader(PATH_TO_LOGFILE);
		Iterator<LogFileEntry> entries = reader.iterator();
		
		int counter = 0;
		while (entries.hasNext()) {
			LogFileEntry entry = entries.next();
			Assert.assertTrue(entry.getTimestamp().getTime() > 0);
			Assert.assertNotNull(entry.getHTTPCode()); // Just make sure the method gets called
			Assert.assertTrue(entry.getDownloadSize() > -1);
			Assert.assertFalse(entry.getURL().isEmpty());
			Assert.assertNotNull(entry.getContentType());
			Assert.assertNotNull(entry.getWorkerThread());
			Assert.assertNotNull(entry.getSHA1Hash());
			counter++;
		}
		
		Assert.assertEquals(1000, counter);
		System.out.println("Done - " + counter + " lines.");
	}
	
	@Test
	public void testIncrementalLogReader() throws IOException {
		IncrementalLogfileReader reader = new IncrementalLogfileReader(PATH_TO_LOGFILE);
		Iterator<LogFileEntry> entries = reader.newIterator();
		
		int counter = 0;
		while (entries.hasNext()) {
			entries.next();
			counter++;
		}
		
		Assert.assertEquals(1000, counter);
		System.out.println("Done - " + counter + " lines.");
	}
	
}
