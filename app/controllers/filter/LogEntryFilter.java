package controllers.filter;

import java.util.ArrayList;
import java.util.List;

import uk.bl.monitrix.heritrix.LogEntry;

public class LogEntryFilter {
	
	public String url;
	
	public long timestamp;
	
	public LogEntryFilter(LogEntry entry) {
		this.url = entry.getURL();
		this.timestamp = entry.getTimestamp().getTime();
	}
	
	public static List<LogEntryFilter> map(List<LogEntry> log) {
		List<LogEntryFilter> mapped = new ArrayList<LogEntryFilter>();
		for (LogEntry entry : log) {
			mapped.add(new LogEntryFilter(entry));
		}
		return mapped;
	}

}
