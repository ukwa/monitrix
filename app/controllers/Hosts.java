package controllers;

import java.awt.geom.Point2D;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.node.ObjectNode;

import scala.concurrent.duration.Duration;
import controllers.mapping.TimeseriesValueMapper;
import play.Logger;
import play.cache.Cache;
import play.libs.Akka;
import play.libs.Json;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.analytics.HostAnalytics;
import uk.bl.monitrix.analytics.LogAnalytics;
import uk.bl.monitrix.analytics.TimeseriesValue;
import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.CrawlLogEntry;
import uk.bl.monitrix.model.KnownHost;
import uk.bl.monitrix.model.KnownHostList;

public class Hosts extends AbstractController {
	
	private static DBConnector db = Global.getBackend();
	
	private static KnownHostList hosts = db.getKnownHostList();
	
	public static Result index() {
		List<String> cappedHosts = db.getCrawlLog().extractHostsForAnnotation(CrawlLogEntry.ANNOTATION_CAPPED_CRAWL);
		return ok(views.html.hosts.index.render(db.getKnownHostList(), cappedHosts));
	}
	
	public static Result getAvergageDelayHistogram() {
		int intervals = getQueryParamAsInt("intervals", 100);
		int increment = (int) (hosts.getMaxFetchDuration() / intervals);
		
		List<Point2D> histogram = new ArrayList<Point2D>();
		for (int i=0; i<=intervals; i++) {
			int from = i * increment;
			int to = from + increment;
			histogram.add(new Point2D.Double(from, hosts.searchByAverageFetchDuration(from, to, 0, 0).totalResults()));
		}
		
		return ok(Json.toJson(histogram));
	}
	
	public static Result getAvergageRetriesHistogram() {
		int intervals = getQueryParamAsInt("maxRetries", 20);
		
		List<Point2D> histogram = new ArrayList<Point2D>();
		for (int r=0; r<intervals; r++) {
			histogram.add(new Point2D.Double(r, hosts.searchByAverageRetries(r, r + 1, 0, 0).totalResults()));
		}
		
		return ok(Json.toJson(histogram));
	}
	
	public static Result getRobotsHistogram() {
		int intervals = getQueryParamAsInt("intervals", 100);
		double increment = 1.0 / intervals;
		
		List<Point2D> histogram = new ArrayList<Point2D>();
		for (int i=0; i<=intervals; i++) {
			double min = increment * i;
			double max = min + increment;
			histogram.add(new Point2D.Double(min, hosts.searchByRobotsBlockPercentage(min, max, 0, 0).totalResults()));
		}
		
		return ok(Json.toJson(histogram));
	}

	public static Result getRedirectsHistogram() {
		int intervals = getQueryParamAsInt("intervals", 100);
		double increment = 1.0 / intervals;
		
		List<Point2D> histogram = new ArrayList<Point2D>();
		for (int i=0; i<=intervals; i++) {
			double min = increment * i;
			double max = min + increment;
			histogram.add(new Point2D.Double(min, hosts.searchByRedirectPercentage(min, max, 0, 0).totalResults()));
		}
		
		return ok(Json.toJson(histogram));
	}
	
	public static Result getHostInfo(String hostname) {
		KnownHost knownHost = db.getKnownHostList().getKnownHost(hostname);
		
		if (knownHost == null) {
			return notFound(); // TODO error handling
		} else {
			Logger.debug("Computed redirect percentage: " + HostAnalytics.computePercentagOfRedirects(knownHost));
			Logger.debug("Stored redirect percentage: " + knownHost.getRedirectPercentage());
			Logger.debug("Computed robots block percentage: " + HostAnalytics.computePercentageOfRobotsTxtBlocks(knownHost));
			Logger.debug("Stored robots block percentage: " + knownHost.getRobotsBlockPercentage());
			
			return ok(views.html.hosts.hostinfo.render(knownHost, db.getCrawlLog(), db.getAlertLog()));
		}
	}
	
	public static Result getURLHistoryForHost(final String hostname) {
		// This computation may take LONG. We'll run it in the background and store status (and result) in the Play cache
		final int maxpoints = getQueryParamAsInt("maxpoints", 100);
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
			},Akka.system().dispatcher());
		
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
