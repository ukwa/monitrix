package controllers;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import controllers.mapping.TimeseriesValueMapper;

import play.Logger;
import play.libs.Json;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.analytics.LogAnalytics;
import uk.bl.monitrix.analytics.TimeseriesValue;
import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.model.CrawlLogEntry;
import uk.bl.monitrix.model.KnownHost;

public class Hosts extends AbstractController {
	
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
			return ok(views.html.hosts.hostinfo.render(knownHost, db.getCrawlLog(), db.getAlertLog()));
		}
	}
	
	public static Result getURLHistoryForHost(String hostname) {
		int total = (int) db.getCrawlLog().countEntriesForHost(hostname);
		Iterator<CrawlLogEntry> iterator = db.getCrawlLog().getEntriesForHost(hostname);
		Collection<CrawlLogEntry> entries = new ArrayDeque<CrawlLogEntry>(total);

		int tenPercent = total / 10;
		int ctr = 0;
		while (iterator.hasNext()) {
			entries.add(iterator.next());
			ctr++;
			if (tenPercent > 0 && ctr % tenPercent == 0)
				Logger.info(ctr / tenPercent + "0% fetched from DB");
		}
		
		List<TimeseriesValue> timeseries = LogAnalytics.getCrawledURLsHistory(entries, getIntParam("maxpoints", 100));
		return ok(Json.toJson(TimeseriesValueMapper.map(timeseries)));
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
