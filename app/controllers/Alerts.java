package controllers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.model.Alert;
import uk.bl.monitrix.model.AlertLog;

public class Alerts extends Controller {
	
	private static AlertLog alertLog = Global.getBackend().getAlertLog();
	
	public static Result index() {
		// TODO improve - pagination?
		Iterator<Alert> it = alertLog.listAll();
		
		List<Alert> alerts = new ArrayList<Alert>();
		while (it.hasNext())
			alerts.add(it.next());
		
		return ok(views.html.alerts.index.render(alerts));
	}
	
}
