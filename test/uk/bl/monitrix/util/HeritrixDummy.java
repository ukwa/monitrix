package uk.bl.monitrix.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import uk.bl.monitrix.heritrix.LogFileEntry;
import uk.bl.monitrix.heritrix.SimpleLogfileReader;

/**
 * The Heritrix Dummy 'simulates' a crawl in progress by replaying an existing Heritrix log ("source log")
 * into a dummy output log ("dummy log"). Timestamps from the source log will be replaced by current wall
 * clock timestamps.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class HeritrixDummy {
	
	// Heritrix Date format
	private static DateFormat ISO_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	// Path to the source log file to replay
	private static final String PATH_TO_SOURCE_LOG = "/home/simonr/Downloads/crawl.log.20120914182409";
	
	// Config: how many dummy crawler logs should be generated?
	private static final int NUMBER_OF_CRAWLERS = 4;
	
	// Dummy log path template
	private static final String PATH_TO_DUMMY_LOG = "/home/simonr/dummy@@number@@.log";
	
	// The approx. amount of time between two log events in the dummy log, in millis (we will add a bit of random jitter)
	private static final int APPROX_LOG_INTERVAL_MS = 10;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		List<BufferedWriter> writers = new ArrayList<BufferedWriter>();
		for (int i=0; i<NUMBER_OF_CRAWLERS; i++) {
			File log = new File(PATH_TO_DUMMY_LOG.replace("@@number@@", Integer.toString(i)));
			writers.add(new BufferedWriter(new FileWriter(log, true)));
		}

		Iterator<LogFileEntry> sourceLog = new SimpleLogfileReader(PATH_TO_SOURCE_LOG).iterator();		
		Random rnd = new Random();
		while (sourceLog.hasNext()) {
			for (BufferedWriter writer : writers) {
				if (sourceLog.hasNext()) {
					String entry = sourceLog.next().toString();
					
					// Rewrite timestamp
					String rewritten = ISO_FORMAT.format(new Date()) + entry.substring(entry.indexOf(' '));
					writer.append(rewritten + "\n");
					writer.flush();
				}
			}
			
			// Wait
			int jitter = rnd.nextInt(2 * APPROX_LOG_INTERVAL_MS) - APPROX_LOG_INTERVAL_MS;
			Thread.sleep(APPROX_LOG_INTERVAL_MS + Math.abs(jitter));
		}
	}
	
}
