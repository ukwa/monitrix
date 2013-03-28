package uk.bl.monitrix.extensions.imageqa.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import uk.bl.monitrix.extensions.imageqa.model.ImageQALogEntry;

public class CsvImageQALog {
	
	private File log;
	
	public CsvImageQALog(String path) throws IOException {
		log = new File(path);
		if (!log.exists())
			throw new IOException();
	}
	
	public Iterator<ImageQALogEntry> iterator() {
		try {
			return new QATextLogIterator(log);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private class QATextLogIterator implements Iterator<ImageQALogEntry> {
		
		private FileInputStream is;
		
		private BufferedReader reader;

		private String nextLine;
		
		QATextLogIterator(File log) throws IOException {
			is = new FileInputStream(log);
			reader = new BufferedReader(new InputStreamReader(is));
			nextLine = reader.readLine();
		}
		
		@Override
		public boolean hasNext() {
			return nextLine != null;
		}

		@Override
		public ImageQALogEntry next() {
			try {
				CsvImageQALogEntry next = new CsvImageQALogEntry(nextLine);
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
