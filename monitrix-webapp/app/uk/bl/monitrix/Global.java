package uk.bl.monitrix;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import play.*;
import play.mvc.*;
import play.mvc.Http.*;
import play.libs.Akka;
import play.libs.F.Promise;
import static play.mvc.Results.*;
import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.database.cassandra.CassandraDBConnector;
import uk.bl.monitrix.database.cassandra.ingest.CassandraDBIngestConnector;
import uk.bl.monitrix.heritrix.api.HeritrixAPI;
import uk.bl.monitrix.heritrix.ingest.IngestWatcher;

/**
 * The Play! Global object.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class Global extends GlobalSettings {

	private static DBConnector db = null;
	
	private static IngestWatcher ingestWatcher = null;
	
	// TODO persist registered API endpoints in DB!
	private static List<HeritrixAPI> crawlers = new ArrayList<HeritrixAPI>();

	private void connectBackend() {
		try {
			db = new CassandraDBConnector();
			
			ingestWatcher = new IngestWatcher(new CassandraDBIngestConnector(db), Akka.system());
			ingestWatcher.startWatching();
			
			Logger.info("Database connected");
		} catch (Exception e) {
			Logger.error("FATAL - could not connect to database "+e);
			e.printStackTrace();
		}
	}

	/**
	 * Returns the database read connector or <code>null</code> if the DB connection failed
	 * for whatever reason. (Check the logs.) 
	 * @return the database read connector or <code>null</code>
	 */
	public static DBConnector getBackend() {
		return db;
	}
	
	/**
	 * Returns the ingest watcher, which is in charge of conducting periodic log-to-database syncs.
	 * @return the ingest watcher
	 */
	public static IngestWatcher getIngestWatcher() {
		return ingestWatcher;
	}
	
	/**
	 * Returns the configured Heritrix crawler APIs, in the form of a map {:endpointURL API}. 
	 * @return the crawler APIs
	 */
	public static List<HeritrixAPI> getCrawlerAPIs() {
		return crawlers;
	}
	
	@Override
	public void onStop(Application app) {
		ingestWatcher.stopWatching();
		if (db != null) {
			db.close();
			Logger.info("Database disconnected");
		}
	}
	
	/**
	 * Redirect all errors (i.e. RuntimeExceptions) to a custom error page.
	 */
	
	public Promise<SimpleResult> onError(RequestHeader request, Throwable t) {
		t.printStackTrace();
		while(t.getCause() != null)
			t = t.getCause();

        return Promise.<SimpleResult>pure(internalServerError(        		
        		views.html.error.generalServerError.render(t)
        ));
    }
	
	/**
	 * In case the DB is not connected, montrix redirects to a specific error
	 * page with extra DB connection instructions.
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public Action onRequest(Request request, Method actionMethod) {
		if (db == null) {
			connectBackend();
			if (db == null)
				return new Action.Simple() {
					@Override
					public Promise<SimpleResult> call(Context arg0) throws Throwable {
						return Promise.<SimpleResult>pure(ok(views.html.error.dbConnectError.render()));
					}
				};
		}
		
		return super.onRequest(request, actionMethod);
	}
}