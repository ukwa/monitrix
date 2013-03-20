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
	
	private static long TEN_MINUTES = 60 * 10000;
	
	private File logFile;
	
	private BufferedReader reader;
	
	private long linesRead = 0;
	
	private long lastModifiedValueAtLastRead = 0;
	
	private long lastSize = 0;

	public IncrementalLogfileReader(String filename) throws FileNotFoundException {
		File log = new File(filename);
		if (!log.exists())
			throw new FileNotFoundException(filename + " not found");

		this.logFile = new File(log.getAbsolutePath());
		this.lastSize = logFile.length();
		
		this.reader = new BufferedReader(new FileReader(log));
	}
	
	public String getPath() {
		return logFile.getAbsolutePath();
	}
	
	public boolean isRenamed() throws IOException {
		// If the log has become smaller - RENAMED!
		if (logFile.length() < lastSize)
			return true;

		// If the log was modified, but the reader didn't read anything in the past 10 minutes - RENAMED!
		if (lastModifiedValueAtLastRead < logFile.lastModified() - TEN_MINUTES)
			return true;

		return false;
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
				LogFileEntry next = new LogFileEntry(logFile.getAbsolutePath(), nextLine);
				nextLine = reader.readLine();
				linesRead++;
				lastModifiedValueAtLastRead = logFile.lastModified();
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
