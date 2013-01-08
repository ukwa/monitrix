package controllers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import global.Global;
import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.Alert;
import uk.bl.monitrix.AlertLog;

public class Alerts extends Controller {
	
	private static AlertLog alertLog = Global.getBackend().getAlertLog();
	
	public static Result index() {
		List<Alert> alerts = new ArrayList<Alert>();
		Iterator<Alert> it = alertLog.listAll();
		while (it.hasNext())
			alerts.add(it.next());
		
		return ok(views.html.alerts.index.render(alerts));
	}
	
}
