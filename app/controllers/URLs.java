package controllers;

import java.util.List;

import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.CrawlLogEntry;

public class URLs extends Controller {
	
	private static CrawlLog crawlLog  = Global.getBackend().getCrawlLog();
	
	public static Result getURLInfo(String url) {
		List<CrawlLogEntry> entries = crawlLog.getEntriesForURL(url);
		return ok(views.html.urls.index.render(entries));
	}
	
	// TODO JSON output for this!

}
