package controllers.mapping;

import java.util.AbstractList;
import java.util.List;

import uk.bl.monitrix.model.CrawlLogEntry;

/**
 * A simple class that wraps a {@link LogFileEntry} so that it can be directly 
 * serialized to JSON by Play. 
 * 
 * TODO complete!
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CrawlLogEntryMapper {
	
	public long timestamp;
	
	public String url;
	
	public long download_size;
	
	public CrawlLogEntryMapper(CrawlLogEntry entry) {
		this.url = entry.getURL();
		this.timestamp = entry.getTimestamp().getTime();
		this.download_size = entry.getDownloadSize();
	}
	
	/**
	 * Utility method to lazily map a list of {@link LogFileEntry} objects
	 * to a list of JSON-compatible wrappers.
	 * @param log the log entries
	 * @return the wrapped list
	 */
	public static List<CrawlLogEntryMapper> map(final List<CrawlLogEntry> log) {
		return new AbstractList<CrawlLogEntryMapper>() {
			
			@Override
			public CrawlLogEntryMapper get(int index) {
				return new CrawlLogEntryMapper(log.get(index));
			}

			@Override
			public int size() {
				return log.size();
			}
			
		};
	}

}
