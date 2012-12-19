package uk.bl.monitrix.db.mongodb;

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
import uk.bl.monitrix.db.mongodb.heritrixlog.HeritrixLogCollection;
import uk.bl.monitrix.db.mongodb.heritrixlog.HeritrixLogDBO;
import uk.bl.monitrix.db.mongodb.knownhosts.KnownHostsDBO;

public class MongoBackedHostInformation implements HostInformation {
	
	private KnownHostsDBO dbo;
	
	private HeritrixLogCollection log;
	
	public MongoBackedHostInformation(KnownHostsDBO dbo, HeritrixLogCollection log) {
		this.dbo = dbo;
		this.log = log;
	}

	@Override
	public String getHostname() {
		return dbo.getHostname();
	}

	@Override
	public long getLastAccess() {
		return dbo.getLastAccess();
	}

	@Override
	public List<String> getCrawlers() {
		Set<String> crawlers = new HashSet<String>();

		Iterator<HeritrixLogDBO> entries = log.getEntriesForHost(dbo.getHostname());		
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
		
		Iterator<HeritrixLogDBO> entries = log.getEntriesForHost(dbo.getHostname());
		while (entries.hasNext()) {
			HeritrixLogDBO dbo = entries.next();
			
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
