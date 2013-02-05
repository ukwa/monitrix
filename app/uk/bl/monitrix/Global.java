package uk.bl.monitrix;

import java.lang.reflect.Method;
import java.util.Iterator;

import play.Application;
import play.GlobalSettings;
import play.Logger;
import play.libs.Akka;
import play.mvc.Action;
import play.mvc.Http.RequestHeader;
import play.mvc.Result;
import play.mvc.Http.Context;
import play.mvc.Http.Request;
import play.mvc.Results;
import uk.bl.monitrix.database.DBBatchImporter;
import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.database.mongodb.MongoDBConnector;
import uk.bl.monitrix.heritrix.IngestorPool;
import uk.bl.monitrix.heritrix.LogFileEntry;

/**
 * The Play! Global object.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class Global extends GlobalSettings {
	
	private static DBConnector db = null;
	
	private static IngestorPool ingestorPool = null;
	
	private void connectBackend() {
		try {
			db = new MongoDBConnector();
			
			// TODO attach MongoDB rather than the dummy! 
			ingestorPool = new IngestorPool(new DBBatchImporter() {
				@Override
				public void insert(Iterator<LogFileEntry> iterator) {
					while (iterator.hasNext())
						iterator.next();
				}
			}, Akka.system());
			Logger.info("Database connected");
		} catch (Exception e) {
			Logger.error("FATAL - could not connect to MongoDB");
		}		
	}
	
	public static DBConnector getBackend() {
		return db;
	}
	
	public static IngestorPool getIngestorPool() {
		return ingestorPool;
	}
	
	@Override
	public void onStop(Application app) {
		if (db != null) {
			Logger.info("Database disconnected");
			db.close();
		}
	}
	
	/**
	 * Redirect all errors (i.e. RuntimeExceptions) to a custom error page.
	 */
	@Override
	public Result onError(RequestHeader request, Throwable t) {
		t.printStackTrace();
		while(t.getCause() != null)
			t = t.getCause();
			
		return Results.ok(views.html.error.generalServerError.render(t));
	}
	
	/**
	 * In case the DB is not connected, montrix redirects to a specific error
	 * page with extra DB conntection instructions.
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public Action onRequest(Request request, Method actionMethod) {
		if (db == null) {
			connectBackend();
			if (db == null)
				return new Action.Simple() {
					@Override
					public Result call(Context arg0) throws Throwable {
						return ok(views.html.error.dbConnectError.render());
					}
				};
		}
		
		return super.onRequest(request, actionMethod);
	}
	
}
