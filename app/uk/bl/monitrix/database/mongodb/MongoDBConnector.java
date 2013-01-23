package uk.bl.monitrix.database.mongodb;

import java.io.IOException;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.Mongo;

import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.database.mongodb.model.MongoAlertLog;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlLog;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlStats;
import uk.bl.monitrix.database.mongodb.model.MongoKnownHostList;
import uk.bl.monitrix.database.mongodb.model.MongoVirusLog;
import uk.bl.monitrix.model.AlertLog;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.CrawlStats;
import uk.bl.monitrix.model.KnownHost;
import uk.bl.monitrix.model.KnownHostList;
import uk.bl.monitrix.model.VirusLog;

/**
 * A MongoDB-backed implementation of {@link DBConnector}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class MongoDBConnector implements DBConnector {
	
	// MongoDB host
	private Mongo mongo;

	// Monitrix database
	private DB db;
	
	// Crawl log
	private CrawlLog crawlLog;
	
	// Crawl stats
	private CrawlStats crawlStats;
	
	// Known hosts list
	private KnownHostList knownHosts;
	
	// Alert log
	private AlertLog alertLog;
	
	// Virus log
	private VirusLog virusLog;
	
	public MongoDBConnector() throws IOException {
		init(MongoProperties.DB_HOST, MongoProperties.DB_NAME, MongoProperties.DB_PORT);
	}
	
	public MongoDBConnector(String hostName, String dbName, int dbPort) throws IOException {
		init(hostName, dbName, dbPort);
	}
	
	private void init(String hostName, String dbName, int dbPort) throws IOException {
		this.mongo = new Mongo(hostName, dbPort);
		this.db = mongo.getDB(dbName);
		
		this.crawlLog = new MongoCrawlLog(db);
		this.crawlStats = new MongoCrawlStats(db);
		this.knownHosts = new MongoKnownHostList(db);
		this.alertLog = new MongoAlertLog(db);
		this.virusLog = new MongoVirusLog(db);
	}

	@Override
	public CrawlLog getCrawlLog() {
		return crawlLog;
	}

	@Override
	public CrawlStats getCrawlStats() {
		return crawlStats;
	}

	@Override
	public AlertLog getAlertLog() {
		return alertLog;
	}
	
	@Override
	public VirusLog getVirusLog() {
		return virusLog;
	}

	@Override
	public KnownHost getKnownHost(String hostname) {
		return knownHosts.getKnownHost(hostname);
	}
	
	@Override
	public List<String> searchHosts(String query) {
		return knownHosts.searchHost(query);
	}

	@Override
	public void close() {
		this.mongo.close();
	}

}
