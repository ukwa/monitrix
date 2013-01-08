package uk.bl.monitrix;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An alert log interface. Provides access to the alert flags stored in the database.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface AlertLog {
	
	public Map<String, List<Alert>> groupedByHost();
	
	public Iterator<Alert> listAll();
	
	public long countAll();

}
