package uk.bl.monitrix.model;

import java.util.Iterator;
import java.util.List;


/**
 * The alert log interface. An alert log provides read/query access to all 
 * alerts stored in the backend.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface AlertLog {
		
	/**
	 * Returns the total number of alerts stored in the alert log.
	 * @return the total number of alerts
	 */
	public long countAll();
	
	/**
	 * Returns an iterator over all alerts stored in the alert log.
	 * @return the alerts
	 */
	public Iterator<Alert> listAll();
	
	/**
	 * Returns the N most recent alerts.
	 * @param n the number of alerts to return
	 * @return the list of alerts
	 */
	public List<Alert> getMostRecent(int n);
	
	/**
	 * Returns the names of offending hosts listed in the alert log.
	 * @return the offending host names
	 */
	public List<String> getOffendingHosts();
	
	/**
	 * Returns the total number of alerts stored for a particular host.
	 * @param hostname the hostname
	 * @return the number of alerts stored for the host
	 */
	public long countAlertsForHost(String hostname);
	
	/**
	 * Returns an iterator over the alerts stored for a particular host.
	 * @return the alerts
	 */
	public Iterator<Alert> listAlertsForHost(String hostname);

}
