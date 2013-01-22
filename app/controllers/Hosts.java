package controllers;

import java.util.List;

import play.mvc.Result;
import play.mvc.Controller;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.model.KnownHost;

public class Hosts extends Controller {
	
	private static final String QUERY = "query";
	
	private static DBConnector db = Global.getBackend();
	
	public static Result searchHosts() {
		String query = getQueryParam(QUERY);
		if (query == null) {
			// TODO error handling
			return notFound();
		} else {
			long startTime = System.currentTimeMillis();
			List<String> hosts = db.searchHosts(query);
			if (hosts.size() == 1)
				return redirect(routes.Hosts.getHostInfo(hosts.get(0)));
			else
				return ok(views.html.hosts.searchResult.render(query, db.searchHosts(query), (System.currentTimeMillis() - startTime), db.getCrawlLog()));
		}
	}
	
	public static Result getHostInfo(String hostname) {
		KnownHost knownHost = db.getKnownHost(hostname);
		
		if (knownHost == null) {
			return notFound(); // TODO error handling
		} else {
			return ok(views.html.hosts.hostinfo.render(knownHost, db.getCrawlLog()));
		}
	}
	
	private static String getQueryParam(String key) {
		String[] value = request().queryString().get(key);
		if (value == null)
			return null;
		
		if (value.length == 0)
			return null;
		
		return value[0];
	}

}
