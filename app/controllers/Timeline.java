package controllers;

import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.Global;

public class Timeline extends Controller {

	public static Result index() {
		return ok(views.html.timeline.render(Global.getCrawlStatistics()));
	}
	
	public static Result getDatavolume() {
		return ok();
	}
	
	public static Result getURLs() {
		return null;
	}
	
	public static Result getHosts() {
		return null;
	}
	
}
