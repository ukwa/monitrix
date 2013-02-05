package controllers.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import uk.bl.monitrix.heritrix.IngestorStatus;

public class LogTrackerStatusMapper {
	
	public String path;
	
	public IngestorStatus status;
	
	public LogTrackerStatusMapper(String path, IngestorStatus status) {
		this.path = path;
		this.status = status;
	}
	
	public static List<LogTrackerStatusMapper> map(Map<String, IngestorStatus> status) {
		List<LogTrackerStatusMapper> mapped = new ArrayList<LogTrackerStatusMapper>();

		for (Entry<String, IngestorStatus> entry : status.entrySet()) {
			mapped.add(new LogTrackerStatusMapper(entry.getKey(), entry.getValue()));
		}
		
		return mapped;
	}

}
