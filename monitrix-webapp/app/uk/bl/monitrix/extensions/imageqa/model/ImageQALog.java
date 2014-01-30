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
	
	/**
	 * Returns the log entry for a specific Web page URL, if it exists
	 * @param url the Web page URL
	 * @return the log entry or <code>null</code>
	 */
	public ImageQALogEntry findForURL(String url);

}
