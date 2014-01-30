package controllers.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import controllers.Admin;

import uk.bl.monitrix.heritrix.ingest.IngestStatus;
import uk.bl.monitrix.model.IngestSchedule;

/**
 * JSON serialization wrapper for ingest status information (as used in the {@link Admin} controller).
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class IngestStatusMapper {
	
	public String id;
	
	public IngestStatus status;
	
	public long lines_ingested;
	
	public IngestStatusMapper(String id, IngestStatus status, long linesIngested) {
		this.id = id;
		this.status = status;
		this.lines_ingested = linesIngested;
	}
	
	/**
	 * Utility method to map ingest status information to a list of JSON-compatible wrappers.
	 * @param status ingest status information
	 * @return the wrapped list
	 */
	public static List<IngestStatusMapper> map(Map<String, IngestStatus> status, IngestSchedule schedule) {
		List<IngestStatusMapper> mapped = new ArrayList<IngestStatusMapper>();

		for (Entry<String, IngestStatus> entry : status.entrySet()) {
			String id = entry.getKey();
			long ingestedLines = (schedule.getLog(id) == null) ? 0 : schedule.getLog(id).getIngestedLines();
			mapped.add(new IngestStatusMapper(id, entry.getValue(), ingestedLines));
		}
		
		return mapped;
	}

}
