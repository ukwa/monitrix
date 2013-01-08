package uk.bl.monitrix.db.mongodb.read;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import uk.bl.monitrix.Alert;
import uk.bl.monitrix.AlertLog;
import uk.bl.monitrix.db.mongodb.model.alerts.AlertsCollection;
import uk.bl.monitrix.db.mongodb.model.alerts.AlertsDBO;

public class MongoBackedAlertLog implements AlertLog {
	
	private AlertsCollection collection;
	
	public MongoBackedAlertLog(AlertsCollection collection) {
		this.collection = collection;
	}
	
	@Override
	public Map<String, List<Alert>> groupedByHost() {
		Map<String, List<Alert>> grouped = new HashMap<String, List<Alert>>();
		
		for (String hostname : collection.getOffendingHosts()) {
			Iterator<AlertsDBO> alerts = collection.getAlertsForHost(hostname);
			
			List<Alert> mapped = new ArrayList<Alert>();
			while (alerts.hasNext())
				mapped.add(map(alerts.next()));
			
			grouped.put(hostname, mapped);
		}
		
		return grouped;
	}

	@Override
	public Iterator<Alert> listAll() {
		final Iterator<AlertsDBO> alerts = collection.listAll();
		
		return new Iterator<Alert>() {
			@Override
			public boolean hasNext() {
				return alerts.hasNext();
			}

			@Override
			public Alert next() {
				return map(alerts.next());
			}

			@Override
			public void remove() {
				alerts.remove();
			}
		};
	}
	
	@Override
	public long countAll() {
		return collection.countAll();
	}
	
	private Alert map(AlertsDBO alert) {
		return new Alert(alert.getOffendingHost(), alert.getAlertName(), alert.getAlertDescription());
	}

}
