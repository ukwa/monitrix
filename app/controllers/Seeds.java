package controllers;

import global.Global;
import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.CrawlStatistics;

public class Seeds extends Controller {
	
	private static CrawlStatistics stats = Global.getCrawlStatistics();
	
	public static Result index() {
		return ok(views.html.seeds.index.render(stats));
	}
	
}
