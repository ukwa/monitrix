package controllers.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import controllers.Admin;

import uk.bl.monitrix.heritrix.ingest.IngestStatus;

/**
 * JSON serialization wrapper for ingest status information (as used in the {@link Admin} controller).
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class IngestStatusMapper {
	
	public String path;
	
	public IngestStatus status;
	
	public IngestStatusMapper(String path, IngestStatus status) {
		this.path = path;
		this.status = status;
	}
	
	/**
	 * Utility method to map ingest status information to a list of JSON-compatible wrappers.
	 * @param status ingest status information
	 * @return the wrapped list
	 */
	public static List<IngestStatusMapper> map(Map<String, IngestStatus> status) {
		List<IngestStatusMapper> mapped = new ArrayList<IngestStatusMapper>();

		for (Entry<String, IngestStatus> entry : status.entrySet()) {
			mapped.add(new IngestStatusMapper(entry.getKey(), entry.getValue()));
		}
		
		return mapped;
	}

}
