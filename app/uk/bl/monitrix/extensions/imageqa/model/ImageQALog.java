package uk.bl.monitrix.extensions.imageqa.model;

import java.util.Iterator;

public interface ImageQALog {
	
	/**
	 * Returns the total number of log entries.
	 * @return the total number of log entries
	 */
	public long countEntries();
	
	/**
	 * Returns an iterator over all QA log entries.
	 * @return the iterator
	 */
	public Iterator<ImageQALogEntry> listEntries();

}
