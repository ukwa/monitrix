package controllers;

import global.Global;
import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.AlertLog;

public class Alerts extends Controller {
	
	private static AlertLog alertLog = Global.getBackend().getAlertLog();
	
	public static Result index() {
		return ok(views.html.alerts.index.render(alertLog.groupedByHost()));
	}
	
}
