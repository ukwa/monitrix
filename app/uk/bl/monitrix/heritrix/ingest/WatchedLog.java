package uk.bl.monitrix.heritrix.ingest;

import uk.bl.monitrix.heritrix.IncrementalLogfileReader;
import uk.bl.monitrix.model.IngestedLog;

public class WatchedLog {
	
	private IngestedLog log;
	
	private IncrementalLogfileReader reader;
	
	private long estimatedLineCount;
	
	public WatchedLog(IngestedLog log, IncrementalLogfileReader reader) {
		this.log = log;
		this.reader = reader;
		this.estimatedLineCount = 0;
	}

	public IngestedLog getLogInfo() {
		return log;
	}
	
	public IncrementalLogfileReader getReader() {
		return reader;
	}
	
	public long getEstimatedLineCount() {
		return estimatedLineCount;
	}
	
	public void setEstimatedLineCount(long lines) {
		this.estimatedLineCount = lines;
	}

}
