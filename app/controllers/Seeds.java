package controllers;

import play.mvc.Controller;
import play.mvc.Result;

public class Seeds extends Controller {
	
	public static Result index() {
		return ok(views.html.seeds.index.render());
	}
	
}
