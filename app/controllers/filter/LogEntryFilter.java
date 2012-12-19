package controllers.filter;

import java.util.AbstractList;
import java.util.List;

import uk.bl.monitrix.heritrix.LogEntry;

/**
 * A simple class that wraps a {@link LogEntry} so that it can be directly 
 * serialized to JSON by Play. 
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class LogEntryFilter {
	
	public String url;
	
	public long timestamp;
	
	public LogEntryFilter(LogEntry entry) {
		this.url = entry.getURL();
		this.timestamp = entry.getTimestamp().getTime();
	}
	
	/**
	 * Utility method to lazily map a list of {@link LogEntry} objects
	 * to a list of JSON-compatible wrappers.
	 * @param log the log entries
	 * @return the wrapped list
	 */
	public static List<LogEntryFilter> map(final List<LogEntry> log) {
		return new AbstractList<LogEntryFilter>() {
			
			@Override
			public LogEntryFilter get(int index) {
				return new LogEntryFilter(log.get(index));
			}

			@Override
			public int size() {
				return log.size();
			}
			
		};
	}

}