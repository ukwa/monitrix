package uk.bl.monitrix.heritrix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * Utility class that provides serial read access to a Heritrix log file.
 *  
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class LogfileReader {

	private File log;

	public LogfileReader(String filename) throws FileNotFoundException {
		this.log = new File(filename);
		if (!this.log.exists())
			throw new FileNotFoundException(filename + " not found");
	}

	public Iterator<LogEntry> iterator() {
		try {
			return new LogIterator(log);
		} catch (IOException e) {
			// Should never happen as we've already checked that the file exists
			// in the constructor!
			throw new RuntimeException(e);
		}
	}
	
	private class LogIterator implements Iterator<LogEntry> {
		
		private FileInputStream is;
		
		private BufferedReader reader;

		private String nextLine;
		
		LogIterator(File log) throws IOException {
			is = new FileInputStream(log);
			reader = new BufferedReader(new InputStreamReader(is));
			nextLine = reader.readLine();
		}
		
		@Override
		public boolean hasNext() {
			return nextLine != null;
		}

		@Override
		public LogEntry next() {
			try {
				LogEntry next = new LogEntry(nextLine);
				nextLine = reader.readLine();
				if (nextLine == null)
					is.close();
				return next;
			} catch (IOException e) {
				// Should never happen as we've already checked that the file exists
				// in the constructor!
				throw new RuntimeException(e);
			}
		}

		@Override
		public void remove() {
			// Not supported
			throw new UnsupportedOperationException();
		}
	}
	
}
