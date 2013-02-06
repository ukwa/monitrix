package uk.bl.monitrix.database;

import java.util.Iterator;
import java.util.List;

import uk.bl.monitrix.heritrix.LogFileEntry;

/**
 * A connection interface for write/ingest access to the Monitrix DB.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface DBIngestConnector {
	
	/**
	 * Returns the list of log files (absolute path names) that are ingested in the DB.
	 * @return the list of log file paths
	 */
	public List<String> getIngestedLogs();
	
	/**
	 * Returns the number of log entries that have been ingested for a specific log
	 * @param logPath the absolute log file path
	 * @return the number of entries ingested from that log
	 */
	public long countEntriesForLog(String logPath);
	
	/**
	 * Ingests a batch of log entries.
	 * @param logPath the log file from which the entries are from (absolute path)
	 * @param iterator the entries
	 */
	public void insert(String logPath, Iterator<LogFileEntry> iterator);

}
