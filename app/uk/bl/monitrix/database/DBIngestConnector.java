package uk.bl.monitrix.database;

import java.util.Iterator;

import uk.bl.monitrix.heritrix.LogFileEntry;
import uk.bl.monitrix.model.IngestSchedule;

/**
 * A connection interface for write/ingest access to the Monitrix DB.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface DBIngestConnector {
	
	/**
	 * Returns the ingest schedule.
	 * @return the ingest schedule
	 */
	public IngestSchedule getIngestSchedule();
	
	/**
	 * Ingests a batch of log entries.
	 * @param logPath the log file from which the entries are from (absolute path)
	 * @param iterator the entries
	 */
	public void insert(String logPath, Iterator<LogFileEntry> iterator);

}
