package controllers;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.node.ObjectNode;

import akka.util.Duration;

import controllers.mapping.TimeseriesValueMapper;

import play.Logger;
import play.cache.Cache;
import play.libs.Akka;
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
		String query = getStringParam(QUERY);
		if (query == null) {
			// TODO error handling
			return notFound();
		} else {
			long startTime = System.currentTimeMillis();
			List<String> hosts = db.getKnownHostList().searchHost(query);
			if (hosts.size() == 1)
				return redirect(routes.Hosts.getHostInfo(hosts.get(0)));
			else
				return ok(views.html.hosts.searchResult.render(query, db.getKnownHostList().searchHost(query), (System.currentTimeMillis() - startTime), db.getCrawlLog()));
		}
	}
	
	public static Result getHostInfo(String hostname) {
		KnownHost knownHost = db.getKnownHostList().getKnownHost(hostname);
		
		if (knownHost == null) {
			return notFound(); // TODO error handling
		} else {
			return ok(views.html.hosts.hostinfo.render(knownHost, db.getCrawlLog(), db.getAlertLog()));
		}
	}
	
	public static Result getURLHistoryForHost(final String hostname) {
		// This computation may take LONG. We'll run it in the background and store status (and result) in the Play cache
		final int maxpoints = getIntParam("maxpoints", 100);
		URLHistoryComputation progress = (URLHistoryComputation) Cache.get(hostname);
		
		if (progress == null) {
			final URLHistoryComputation cachedProgress = new URLHistoryComputation();
			Cache.set(hostname, cachedProgress);
			
			// Start async computation
			Akka.system().scheduler().scheduleOnce(Duration.fromNanos(0), new Runnable() {
				@Override
				public void run() {
					int total = (int) db.getCrawlLog().countEntriesForHost(hostname);
					Iterator<CrawlLogEntry> iterator = db.getCrawlLog().getEntriesForHost(hostname);
					Collection<CrawlLogEntry> entries = new ArrayDeque<CrawlLogEntry>(total);

					int tenPercent = total / 10;
					int ctr = 0;
					while (iterator.hasNext()) {
						entries.add(iterator.next());
						ctr++;
						if (tenPercent > 0 && ctr % tenPercent == 0) {
							cachedProgress.progress = (ctr / tenPercent) * 10;
							Logger.info(ctr / tenPercent + "0% fetched from DB");
						}
					}
					
					List<TimeseriesValue> result = LogAnalytics.getCrawledURLsHistory(entries, maxpoints);
					cachedProgress.result = result;
				}
			});
			
			progress = cachedProgress;
		} else {
			if (progress.result != null) {
				List<TimeseriesValue> timeseries = progress.result;
				Cache.set(hostname, null);
				return ok(Json.toJson(TimeseriesValueMapper.map(timeseries)));
			}
		}
		
		ObjectNode json = Json.newObject();
		json.put("progress", progress.progress);
		return ok(json);
	}
	
	private static class URLHistoryComputation {
		
		int progress = 0;
		
		List<TimeseriesValue> result = null;
			
	}

}
