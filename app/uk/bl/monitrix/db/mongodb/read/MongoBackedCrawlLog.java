package uk.bl.monitrix.db.mongodb.read;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

import uk.bl.monitrix.CrawlLog;
import uk.bl.monitrix.CrawlLogEntry;
import uk.bl.monitrix.db.mongodb.model.crawllog.CrawlLogCollection;
import uk.bl.monitrix.db.mongodb.model.crawllog.CrawlLogDBO;

/**
 * An implementation of the {@link CrawlLog} interface backed by the
 * 'Heritrix Log' MongoDB collection.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoBackedCrawlLog implements CrawlLog {
	
	private CrawlLogCollection collection;
	
	public MongoBackedCrawlLog(CrawlLogCollection collection) {
		this.collection = collection;
	}

	@Override
	public List<CrawlLogEntry> getMostRecentEntries(int n) {
		final List<CrawlLogDBO> recent = collection.getMostRecentEntries(n);
		return new AbstractList<CrawlLogEntry>() {

			@Override
			public CrawlLogEntry get(int index) {
				return new CrawlLogEntry(recent.get(index).getLogLine());
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
	public Iterator<CrawlLogEntry> getEntriesForHost(String hostname) {
		final Iterator<CrawlLogDBO> entries = collection.getEntriesForHost(hostname);
		return new Iterator<CrawlLogEntry>() {

			@Override
			public boolean hasNext() {
				return entries.hasNext();
			}

			@Override
			public CrawlLogEntry next() {
				return new CrawlLogEntry(entries.next().getLogLine());
			}

			@Override
			public void remove() {
				entries.remove();
			}
			
		};
	}

}
