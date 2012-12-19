package controllers;

import play.mvc.Controller;
import play.mvc.Result;

public class Alerts extends Controller {
	
	public static Result index() {
		return ok(views.html.alerts.index.render());
	}
	
}
