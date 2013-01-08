package uk.bl.monitrix.db.mongodb.ingest.alertprocessing;

import uk.bl.monitrix.Alert;
import uk.bl.monitrix.CrawlLogEntry;

public class TooManyPathSegmentsRule implements AlertRule {

	private static final String ALERT_NAME = "Too Many Path Segments";
	
	private static final String ALERT_DESCRIPTION = "The following URL has too many path segments: ";
	
	@Override
	public Alert check(CrawlLogEntry entry) {
		String[] pathSegments = entry.getURL().split("/");
		if (pathSegments.length > AlertProperties.TOO_MANY_PATH_SEGMENTS_THRESHOLD)
			return new Alert(entry.getHost(), ALERT_NAME, ALERT_DESCRIPTION + entry.getURL());
		else
			return null;
	}

}
