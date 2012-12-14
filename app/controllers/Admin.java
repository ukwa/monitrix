package controllers;

import play.mvc.Controller;
import play.mvc.Result;

public class Admin extends Controller{

	public static Result index() {
		return ok(views.html.admin.index.render());
	}
	
}
