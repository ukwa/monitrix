package controllers.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import uk.bl.monitrix.heritrix.IngestStatus;

public class IngestStatusMapper {
	
	public String path;
	
	public IngestStatus status;
	
	public IngestStatusMapper(String path, IngestStatus status) {
		this.path = path;
		this.status = status;
	}
	
	public static List<IngestStatusMapper> map(Map<String, IngestStatus> status) {
		List<IngestStatusMapper> mapped = new ArrayList<IngestStatusMapper>();

		for (Entry<String, IngestStatus> entry : status.entrySet()) {
			mapped.add(new IngestStatusMapper(entry.getKey(), entry.getValue()));
		}
		
		return mapped;
	}

}
