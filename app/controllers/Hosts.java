package controllers;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import play.Logger;
import play.mvc.Result;
import play.mvc.Controller;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.database.mongodb.model.MongoCrawlLog;
import uk.bl.monitrix.model.CrawlLogEntry;
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
			long fetchStart = System.currentTimeMillis();
			Logger.info("Fetching log entries for host " + hostname);
			
			int total = (int) db.getCrawlLog().countEntriesForHost(hostname);
			Iterator<CrawlLogEntry> iterator = ((MongoCrawlLog) db.getCrawlLog()).getEntriesForHost(hostname, true);
			Collection<CrawlLogEntry> entries = new ArrayDeque<CrawlLogEntry>(total);
			
			int tenPercent = total / 10;
			int ctr = 0;
			while (iterator.hasNext()) {
				entries.add(iterator.next());
				ctr++;
				if (ctr % tenPercent == 0)
					Logger.info(ctr / tenPercent + "0% fetched from DB");
			}
			
			long took = System.currentTimeMillis() - fetchStart;
			Logger.info("Done - took " + took + "ms");			
			return ok(views.html.hosts.hostinfo.render(knownHost, entries, took));
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
