package uk.bl.monitrix.alerts;

import uk.bl.monitrix.Alert;
import uk.bl.monitrix.heritrix.LogEntry;

public interface AlertRule {
	
	public Alert check(LogEntry entry);

}
