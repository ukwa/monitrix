package uk.bl.monitrix.extensions.imageqa;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

public class QATextLogTest {
	
	private static final String LOG_PATH = "test/image-qa.txt";
	
	@Test
	public void testLogRead() throws IOException {
		QATextLog log = new QATextLog(LOG_PATH);
		Iterator<QATextLogEntry> it = log.iterator();
		while(it.hasNext())
			System.out.println(it.next());
	}

}
