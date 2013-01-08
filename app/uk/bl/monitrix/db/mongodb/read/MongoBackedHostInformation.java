package uk.bl.monitrix.db.mongodb.read;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import uk.bl.monitrix.HostInformation;
import uk.bl.monitrix.PieChartValue;
import uk.bl.monitrix.db.mongodb.model.crawllog.CrawlLogCollection;
import uk.bl.monitrix.db.mongodb.model.crawllog.CrawlLogDBO;
import uk.bl.monitrix.db.mongodb.model.knownhosts.KnownHostsDBO;

/**
 * An implementation of the {@link HostInformation} interface backed by the
 * 'Heritrix Log' MongoDB collection.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoBackedHostInformation implements HostInformation {
	
	private KnownHostsDBO dbo;
	
	private CrawlLogCollection log;
	
	public MongoBackedHostInformation(KnownHostsDBO dbo, CrawlLogCollection log) {
		this.dbo = dbo;
		this.log = log;
	}

	@Override
	public String getHostname() {
		return dbo.getHostname();
	}
	
	@Override
	public long getFirstAccess() {
		return dbo.getFirstAccess();
	}

	@Override
	public long getLastAccess() {
		return dbo.getLastAccess();
	}

	@Override
	public List<String> getCrawlers() {
		Set<String> crawlers = new HashSet<String>();

		Iterator<CrawlLogDBO> entries = log.getEntriesForHost(dbo.getHostname());		
		while (entries.hasNext())
			crawlers.add(entries.next().getCrawlerID());
		
		List<String> sorted = new ArrayList<String>(crawlers);
		Collections.sort(sorted);
		return sorted;
	}

	@Override
	public long countCrawledURLs() {
		return log.countEntriesForHost(dbo.getHostname());
	}

	@Override
	public List<PieChartValue> getHTTPCodes() {
		// (HTTPCode -> NumberOfLines)
		Map<Integer, Integer> codes = new HashMap<Integer, Integer>();
		
		Iterator<CrawlLogDBO> entries = log.getEntriesForHost(dbo.getHostname());
		while (entries.hasNext()) {
			CrawlLogDBO dbo = entries.next();
			
			Integer code = dbo.geHTTPCode();
			Integer value = codes.get(code);
			
			if (value == null)
				value = Integer.valueOf(1);
			else
				value = Integer.valueOf(value.intValue() + 1);
			
			codes.put(code, value);
		}
		
		List<PieChartValue> pieChart = new ArrayList<PieChartValue>();
		for (Entry<Integer, Integer> entry : codes.entrySet())
			pieChart.add(new PieChartValue(entry.getKey().toString(), entry.getValue()));
		
		return pieChart;
	}

}
