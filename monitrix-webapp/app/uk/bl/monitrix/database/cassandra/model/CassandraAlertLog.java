package uk.bl.monitrix.database.cassandra.model;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.poi.hssf.record.formula.functions.Rows;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import uk.bl.monitrix.database.cassandra.CassandraProperties;
import uk.bl.monitrix.model.Alert;
import uk.bl.monitrix.model.Alert.AlertType;
import uk.bl.monitrix.model.AlertLog;

/**
 * A CassandraDB-backed implementation of {@link AlertLog}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CassandraAlertLog implements AlertLog {
	
	protected Session session;
	
	public CassandraAlertLog(Session session) {
		this.session = session;
	}

	@Override
	public long countAll() {
		ResultSet results = session.execute("SELECT COUNT(*) FROM crawl_uris.alerts;");
		return results.one().getLong("count");
	}

	@Override
	public Iterator<Alert> listAll() {
		return map(session.execute("SELECT * FROM crawl_uris.alerts;").iterator());
	}
	
	@Override
	public List<Alert> getMostRecent(int n) {
		Iterator<Row> rows = session.execute("SELECT * FROM crawl_uris.alerts LIMIT "+n+";").iterator();
		
		List<Alert> recent = new ArrayList<Alert>();
		while(rows.hasNext())
			recent.add(new CassandraAlert(rows.next()));

		return recent;
	}

	@Override
	public List<String> getOffendingHosts() {
		Iterator<Row> rows = session.execute("SELECT host FROM crawl_uris.alerts;").iterator();
		Set<String> hosts = new HashSet<String>();
		while(rows.hasNext()) {
			hosts.add( rows.next().getString("host"));
		}
		List<String> hostList = new ArrayList<String>();
		hostList.addAll(hosts);
		return hostList;
	}

	@Override
	public long countAlertsForHost(String hostname) {
		ResultSet results = session.execute("SELECT COUNT(*) FROM crawl_uris.alerts WHERE host='"+hostname+"';");
		return results.one().getLong("count");
	}
	
	@Override
	public long countAlertsForHost(String hostname, AlertType type) {
		ResultSet results = session.execute("SELECT COUNT(*) FROM crawl_uris.alerts WHERE host='"+hostname+"' AND alert_type='"+type+"';");
		return results.one().getLong("count");
	}
	
	@Override
	public List<AlertType> getAlertTypesForHost(String hostname) {
		Iterator<Row> rows = session.execute("SELECT alert_type FROM crawl_uris.alerts WHERE host='"+hostname+"';").iterator();
		Set<AlertType> set = new HashSet<AlertType>();
		while(rows.hasNext()) {
			set.add( AlertType.valueOf(rows.next().getString("alert_type")));
		}
		List<AlertType> list = new ArrayList<AlertType>();
		list.addAll(set);
		return list;
	}


	@Override
	public Iterator<Alert> listAlertsForHost(String hostname) {
		return map(session.execute("SELECT * FROM crawl_uris.alerts WHERE host='"+hostname+"';").iterator());
	}
	
	/**
	 * Utility method that maps a database cursor to an Iterator of Alert domain objects. 
	 * @param cursor the DB cursor
	 * @return the domain objects
	 */
	private static Iterator<Alert> map(final Iterator<Row> cursor) {
		return new Iterator<Alert>() {		
			@Override
			public boolean hasNext() {
				return cursor.hasNext();
			}

			@Override
			public Alert next() {
				return new CassandraAlert(cursor.next());
			}

			@Override
			public void remove() {
				cursor.remove();
			}
		};
	}
	
}
