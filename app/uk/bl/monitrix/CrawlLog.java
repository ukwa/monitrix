package uk.bl.monitrix;

import java.util.List;

import uk.bl.monitrix.heritrix.LogEntry;

public interface CrawlLog {

	public List<LogEntry> getMostRecentEntries(int n);

}
