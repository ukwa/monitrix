package controllers;

import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.model.AlertLog;

public class Alerts extends Controller {
	
	private static AlertLog alertLog = Global.getBackend().getAlertLog();
	
	public static Result index() {
		return ok(views.html.alerts.index.render(alertLog));
	}
	
}
