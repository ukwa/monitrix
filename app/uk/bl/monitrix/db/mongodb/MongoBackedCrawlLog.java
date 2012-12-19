package uk.bl.monitrix.db.mongodb;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

import uk.bl.monitrix.CrawlLog;
import uk.bl.monitrix.db.mongodb.heritrixlog.HeritrixLogCollection;
import uk.bl.monitrix.db.mongodb.heritrixlog.HeritrixLogDBO;
import uk.bl.monitrix.heritrix.LogEntry;

/**
 * An implementation of the {@link CrawlLog} interface backed by the
 * 'Heritrix Log' MongoDB collection.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoBackedCrawlLog implements CrawlLog {
	
	private HeritrixLogCollection collection;
	
	public MongoBackedCrawlLog(HeritrixLogCollection collection) {
		this.collection = collection;
	}

	@Override
	public List<LogEntry> getMostRecentEntries(int n) {
		final List<HeritrixLogDBO> recent = collection.getMostRecentEntries(n);
		return new AbstractList<LogEntry>() {

			@Override
			public LogEntry get(int index) {
				return new LogEntry(recent.get(index).getLogLine());
			}

			@Override
			public int size() {
				return recent.size();
			}
			
		};
	}

	@Override
	public long countEntriesForHost(String hostname) {
		return collection.countEntriesForHost(hostname);
	}

	@Override
	public Iterator<LogEntry> getEntriesForHost(String hostname) {
		final Iterator<HeritrixLogDBO> entries = collection.getEntriesForHost(hostname);
		return new Iterator<LogEntry>() {

			@Override
			public boolean hasNext() {
				return entries.hasNext();
			}

			@Override
			public LogEntry next() {
				return new LogEntry(entries.next().getLogLine());
			}

			@Override
			public void remove() {
				entries.remove();
			}
			
		};
	}

}
