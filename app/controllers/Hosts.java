package controllers;

import global.Global;
import play.mvc.Result;
import play.mvc.Controller;
import uk.bl.monitrix.HostInformation;
import uk.bl.monitrix.db.DBConnector;

public class Hosts extends Controller {
	
	private static DBConnector db = Global.getBackend();
	
	public static Result getHostInfo() {
		String[] query = request().queryString().get("query");
		
		HostInformation hostInfo = null;
		if (query != null && query.length == 1)
			hostInfo = db.getHostInfo(query[0]);

		if (hostInfo == null)
			return notFound(); // TODO error handling
		else
			return ok(views.html.hosts.hostinfo.render(hostInfo));
	}

}
