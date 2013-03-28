package uk.bl.monitrix.database.mongodb;

import java.io.IOException;
import java.util.HashMap;

import com.mongodb.DB;
import com.mongodb.Mongo;

import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.database.ExtensionTable;
import uk.bl.monitrix.database.mongodb.model.MongoAlertLog;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlLog;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlStats;
import uk.bl.monitrix.database.mongodb.model.MongoIngestSchedule;
import uk.bl.monitrix.database.mongodb.model.MongoKnownHostList;
import uk.bl.monitrix.database.mongodb.model.MongoVirusLog;
import uk.bl.monitrix.model.AlertLog;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.CrawlStats;
import uk.bl.monitrix.model.IngestSchedule;
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
	
	// Ingest schedule
	private IngestSchedule ingestSchedule;
	
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
	
	// Extension tables
	private HashMap<String, ExtensionTable> extensionTables = new HashMap<String, ExtensionTable>();
	
	public MongoDBConnector() throws IOException {
		init(MongoProperties.DB_HOST, MongoProperties.DB_NAME, MongoProperties.DB_PORT);
	}
	
	public MongoDBConnector(String hostName, String dbName, int dbPort) throws IOException {
		init(hostName, dbName, dbPort);
	}
	
	private void init(String hostName, String dbName, int dbPort) throws IOException {
		this.mongo = new Mongo(hostName, dbPort);
		this.db = mongo.getDB(dbName);
		
		this.ingestSchedule = new MongoIngestSchedule(db);
		this.crawlLog = new MongoCrawlLog(db);
		this.crawlStats = new MongoCrawlStats(db);
		this.knownHosts = new MongoKnownHostList(db);
		this.alertLog = new MongoAlertLog(db);
		this.virusLog = new MongoVirusLog(db);
	}
	
	@Override
	public IngestSchedule getIngestSchedule() {
		return ingestSchedule;
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
	public KnownHostList getKnownHostList() {
		return knownHosts;
	}

	@Override
	public void close() {
		this.mongo.close();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T extends ExtensionTable> T getExtensionTable(String name, Class<T> type) {
		ExtensionTable ext = extensionTables.get(name);
		
		if (ext == null) {
			try {
				ext = type.getConstructor(DB.class).newInstance(db);
				extensionTables.put(name, ext);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
			
		return (T) ext;
	}

}
