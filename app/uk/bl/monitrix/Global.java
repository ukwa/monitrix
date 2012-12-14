package uk.bl.monitrix;

import java.lang.reflect.Method;

import play.Application;
import play.GlobalSettings;
import play.Logger;
import play.mvc.Action;
import play.mvc.Http.RequestHeader;
import play.mvc.Result;
import play.mvc.Http.Context;
import play.mvc.Http.Request;
import play.mvc.Results;
import uk.bl.monitrix.db.DBConnector;
import uk.bl.monitrix.db.mongodb.MongoConnector;
import uk.bl.monitrix.stats.CrawlStatistics;

/**
 * The Play! Global object.
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class Global extends GlobalSettings {
	
	// Storage backend
	private static DBConnector db = null;
	
	private void connectBackend() {
		try {
			db = new MongoConnector();
			Logger.info("Database connected");
		} catch (Exception e) {
			Logger.error("FATAL - could not connect to MongoDB");
		}		
	}
	
	public static CrawlStatistics getCrawlStatistics() {
		return db.getCrawlStatistics();
	}
	
	@Override
	public void onStop(Application app) {
		if (db != null)
			db.close();
	}
	
	/**
	 * Redirect all errors (i.e. RuntimeExceptions) to a custom error page.
	 */
	@Override
	public Result onError(RequestHeader request, Throwable t) {
		t.printStackTrace();
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
