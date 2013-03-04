package controllers;

import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.model.CrawlLog;

public class CrawlParameters extends Controller {
	
private static DBConnector backend = Global.getBackend();
	
	private static CrawlLog log = backend.getCrawlLog();
	
	public static Result index() {
		return ok(views.html.crawlParameters.index.render(log));
	}
	
}
