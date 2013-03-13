package controllers;

import java.util.List;

import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.model.CrawlLog;

public class CrawlParameters extends Controller {
	
private static DBConnector backend = Global.getBackend();

	private static final String CAPPED_CRAWL_ANNOTATION = "Q:serverMaxSuccessKb";
	
	private static CrawlLog log = backend.getCrawlLog();
	
	public static Result index() {
		List<String> cappedHosts = log.extractHostsForAnnotation(CAPPED_CRAWL_ANNOTATION);
		return ok(views.html.crawlParameters.index.render(cappedHosts));
	}
	
}
