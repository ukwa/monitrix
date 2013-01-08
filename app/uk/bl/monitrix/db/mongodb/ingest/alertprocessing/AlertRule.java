package uk.bl.monitrix.db.mongodb.ingest.alertprocessing;

import uk.bl.monitrix.api.Alert;
import uk.bl.monitrix.api.CrawlLogEntry;

public interface AlertRule {
	
	public Alert check(CrawlLogEntry entry);

}
