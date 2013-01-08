package uk.bl.monitrix.db.mongodb.ingest.alertprocessing;

import uk.bl.monitrix.Alert;
import uk.bl.monitrix.CrawlLogEntry;

public interface AlertRule {
	
	public Alert check(CrawlLogEntry entry);

}
