package controllers;

import controllers.filter.LogEntryFilter;
import global.Global;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.CrawlLog;

public class Home extends Controller {
	
	private static CrawlLog log = Global.getBackend().getCrawlLog();
	
	public static Result index() {
		return ok(views.html.home.index.render());
	}
	
	// TODO not sure where to put this API method...
	public static Result getMostRecent() {
		return ok(Json.toJson(LogEntryFilter.map(log.getMostRecentEntries(100))));
	}
  
}