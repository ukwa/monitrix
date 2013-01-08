package uk.bl.monitrix.alerts;

import uk.bl.monitrix.Alert;
import uk.bl.monitrix.CrawlLogEntry;

public interface AlertRule {
	
	public Alert check(CrawlLogEntry entry);

}
