package controllers;

import play.mvc.Controller;
import play.mvc.Result;

public class Timeline extends Controller {

	public static Result index() {
		return ok(views.html.timeline.render());
	}
	
}
