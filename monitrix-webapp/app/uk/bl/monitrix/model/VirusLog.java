package uk.bl.monitrix.model;

import java.util.Iterator;

/**
 * The Virus Log interface. Provides read/query access to the list
 * of viruses encountered during the crawl.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface VirusLog {
	
	/**
	 * Returns the virus record for the specified virus.
	 * @param virusName the virus name
	 * @return the record
	 */
	public VirusRecord getRecordForVirus(String virusName);
	
	/**
	 * Returns an iterator over all virus records in the system.
	 * 
	 * TODO probably not the ideal way to expose this. Wait and see what the requirements will be for this.
	 * 
	 * @return the list of virus records in the system
	 */
	public Iterator<VirusRecord> getVirusRecords();

}
