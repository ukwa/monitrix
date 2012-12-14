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
import uk.bl.monitrix.db.CrawlStatistics;
import uk.bl.monitrix.db.DBConnector;
import uk.bl.monitrix.db.mongodb.MongoConnector;

public class Global extends GlobalSettings {
	
	private static DBConnector db = null;
	
	private void connectToDB() {
		try {
			db = new MongoConnector();
		} catch (Exception e) {
			Logger.error("FATAL - could not connect to MongoDB");
		}		
	}
	
	public static CrawlStatistics getCrawlStatistics() {
		return db.getCrawlStatistics();
	}
	
	@Override
	public void onStart(Application app) {
		connectToDB();
	}  
	
	@Override
	public void onStop(Application app) {
		if (db != null)
			db.close();
	}
	
	@Override
	public Result onError(RequestHeader request, Throwable t) {
		t.printStackTrace();
		return Results.ok(views.html.error.render(t));
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public Action onRequest(Request request, Method actionMethod) {
		// In case there is no proper DB connection, redirect to specific error page
		if (db == null) {
			connectToDB();
			
			if (db == null) {
				return new Action.Simple() {
					@Override
					public Result call(Context arg0) throws Throwable {
						return ok(views.html.dbNotFound.render());
					}
				};
			}
		}
		
		return super.onRequest(request, actionMethod);
	}
	
}
