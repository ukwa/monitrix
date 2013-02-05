package uk.bl.monitrix.heritrix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * An incremental log file reader that emulates UNIX 'tail -f'-like read behavior.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class IncrementalLogfileReader {
	
	private BufferedReader reader;
	
	private long linesRead = 0;

	public IncrementalLogfileReader(String filename) throws FileNotFoundException {
		File log = new File(filename);
		if (!log.exists())
			throw new FileNotFoundException(filename + " not found");
		
		this.reader = new BufferedReader(new FileReader(log));
	}

	/**
	 * Returns an iterator over all log entries that have not yet been consumed through
	 * this {@link IncrementalLogfileReader} instance (including those that may have been
	 * added to the underlying log file in the mean time).
	 * @return the iterator 
	 */
	public Iterator<LogFileEntry> newIterator() {
		try {
			return new FollowingLogIterator(reader);
		} catch (IOException e) {
			// Should never happen as we've already checked that the file exists
			// in the constructor!
			throw new RuntimeException(e);
		}
	}
	
	public long getNumberOfLinesRead() {
		return linesRead;
	}
	
	private class FollowingLogIterator implements Iterator<LogFileEntry> {
		
		private BufferedReader reader;
		
		private String nextLine;
		
		FollowingLogIterator(BufferedReader reader) throws IOException {
			this.reader = reader;
			nextLine = reader.readLine();
		}
		
		@Override
		public boolean hasNext() {
			return nextLine != null;
		}

		@Override
		public LogFileEntry next() {
			try {
				LogFileEntry next = new LogFileEntry(nextLine);
				nextLine = reader.readLine();
				linesRead++;
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
