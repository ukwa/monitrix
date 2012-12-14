package controllers;

import play.mvc.Controller;
import play.mvc.Result;

public class Timeline extends Controller {

	public static Result index() {
		return ok(views.html.timeline.render());
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
