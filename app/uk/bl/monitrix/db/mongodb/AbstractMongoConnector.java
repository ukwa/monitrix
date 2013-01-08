package uk.bl.monitrix.db.mongodb;

import java.io.IOException;

import uk.bl.monitrix.db.mongodb.model.AlertsCollection;
import uk.bl.monitrix.db.mongodb.model.CrawlLogCollection;
import uk.bl.monitrix.db.mongodb.model.GlobalStatsCollection;
import uk.bl.monitrix.db.mongodb.model.KnownHostsCollection;
import uk.bl.monitrix.db.mongodb.model.PreAggregatedStatsCollection;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * Common initialization code for MonogDB read & ingest connector implementations.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class AbstractMongoConnector {
	
	// MongoDB host
	protected Mongo mongo;
	
	// Monitrix database
	protected DB db;
	
	// Global Stats collection
	protected GlobalStatsCollection globalStatsCollection;
	
	// Heretrix Log collection
	protected CrawlLogCollection heritrixLogCollection;
	
	// Known Hosts collection
	protected KnownHostsCollection knownHosts;
	
	// Pre-Aggregated Stats collection
	protected PreAggregatedStatsCollection preAggregatedStatsCollection;
	
	// Alerts collection
	protected AlertsCollection alertsCollection;
	
	public AbstractMongoConnector() throws IOException {
		init(MongoProperties.DB_HOST, MongoProperties.DB_NAME, MongoProperties.DB_PORT);
	}
	
	public AbstractMongoConnector(String hostName, String dbName, int dbPort) throws IOException {
		init(hostName, dbName, dbPort);
	}
	
	private void init(String hostName, String dbName, int dbPort) throws IOException {
		this.mongo = new Mongo(hostName, dbPort);
		this.db = mongo.getDB(dbName);
		this.globalStatsCollection = new GlobalStatsCollection(db);
		this.heritrixLogCollection = new CrawlLogCollection(db);
		this.knownHosts = new KnownHostsCollection(db);
		this.preAggregatedStatsCollection = new PreAggregatedStatsCollection(db, knownHosts);
		this.alertsCollection = new AlertsCollection(db);
	}

}
