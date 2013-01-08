package uk.bl.monitrix.db.mongodb;

import java.util.Iterator;

import uk.bl.monitrix.Alert;
import uk.bl.monitrix.AlertLog;
import uk.bl.monitrix.db.mongodb.alerts.AlertsCollection;
import uk.bl.monitrix.db.mongodb.alerts.AlertsDBO;

public class MongoBackedAlertLog implements AlertLog {
	
	private AlertsCollection collection;
	
	public MongoBackedAlertLog(AlertsCollection collection) {
		this.collection = collection;
	}

	@Override
	public Iterator<Alert> listAll() {
		final Iterator<AlertsDBO> all = collection.all();
		return new Iterator<Alert>() {
			@Override
			public boolean hasNext() {
				return all.hasNext();
			}

			@Override
			public Alert next() {
				AlertsDBO next = all.next();
				return new Alert(next.getOffendingHost(), next.getAlertName(), next.getAlertDescription());
			}

			@Override
			public void remove() {
				all.remove();
			}
		};
	}

}
