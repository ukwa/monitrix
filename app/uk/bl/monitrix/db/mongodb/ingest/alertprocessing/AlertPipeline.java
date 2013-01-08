package uk.bl.monitrix.db.mongodb.ingest.alertprocessing;

import java.util.ArrayList;
import java.util.List;

import uk.bl.monitrix.api.Alert;
import uk.bl.monitrix.api.CrawlLogEntry;

public class AlertPipeline {
	
	public List<AlertRule> rules = new ArrayList<AlertRule>();
	
	public AlertPipeline() {
		rules.add(new TooManyPathSegmentsRule());
	}
	
	public List<Alert> check(CrawlLogEntry entry) {
		List<Alert> alerts = new ArrayList<Alert>();
		
		for (AlertRule rule : rules) {
			Alert alert = rule.check(entry);
			if (alert != null)
				alerts.add(alert);
		}
		
		return alerts;
	}

}
