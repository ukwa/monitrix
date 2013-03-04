package controllers;

import play.mvc.Controller;
import play.mvc.Result;

public class Heritrix extends Controller {
	
	public static Result index() {
		return ok(views.html.heritrix.index.render());
	}

}
