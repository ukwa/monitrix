package uk.bl.monitrix.db.mongodb.read;

import java.io.IOException;
import java.util.List;

import uk.bl.monitrix.api.AlertLog;
import uk.bl.monitrix.api.CrawlLog;
import uk.bl.monitrix.api.CrawlStatistics;
import uk.bl.monitrix.api.HostInformation;
import uk.bl.monitrix.db.ReadConnector;
import uk.bl.monitrix.db.mongodb.AbstractMongoConnector;
import uk.bl.monitrix.db.mongodb.model.KnownHostsDBO;

/**
 * An implementation of {@link ReadConnector} for MongoDB.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoReadConnector extends AbstractMongoConnector implements ReadConnector {	
	
	public MongoReadConnector() throws IOException {
		super();
	}
	
	public MongoReadConnector(String hostName, String dbName, int dbPort) throws IOException {
		super(hostName, dbName, dbPort);
	}

	@Override
	public CrawlLog getCrawlLog() {
		// Note: this means that every object calling this method gets their own copy, including their own cache!
		// TODO either use a central cache or only create a single MongoBackedCrawlStatistics instance
		return new MongoBackedCrawlLog(heritrixLogCollection);
	}
	
	@Override
	public CrawlStatistics getCrawlStatistics() {
		// Note: this means that every object calling this method gets their own copy, including their own cache!
		// TODO either use a central cache or only create a single MongoBackedCrawlStatistics instance
		return new MongoBackedCrawlStatistics(globalStatsCollection, preAggregatedStatsCollection);
	}
	
	@Override
	public List<String> searchHosts(String query) {
		return knownHosts.searchHost(query);
	}
	
	@Override
	public AlertLog getAlertLog() {
		// Note: this means that every object calling this method gets their own copy, including their own cache!
		// TODO either use a central cache or only create a single MongoBackedCrawlStatistics instance
		return new MongoBackedAlertLog(alertsCollection);
	}
	
	@Override
	public HostInformation getHostInfo(String hostname) {
		KnownHostsDBO dbo = knownHosts.getHostInfo(hostname);
		if (dbo == null)
			return null;
		
		// Note: this means that every object calling this method gets their own copy, including their own cache!
		// TODO either use a central cache or only create a single MongoBackedHostInformation instance
		return new MongoBackedHostInformation(dbo, heritrixLogCollection);
	}
	
	@Override
	public void close() {
		this.mongo.close();
	}

}
