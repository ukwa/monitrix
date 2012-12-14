package controllers;

import play.mvc.Controller;
import play.mvc.Result;

public class Home extends Controller {
  
  public static Result index() {
    return ok(views.html.home.index.render());
  }
  
}