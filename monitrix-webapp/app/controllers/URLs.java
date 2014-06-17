package controllers;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

import play.Logger;
import play.libs.Json;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.database.cassandra.model.CassandraCrawlLog;
import uk.bl.monitrix.database.cassandra.model.CassandraCrawlLog.Tuple;
import uk.bl.monitrix.model.CrawlLogEntry;

public class URLs extends AbstractController {
	
	private static CassandraCrawlLog crawlLog  = (CassandraCrawlLog) Global.getBackend().getCrawlLog();
	
	public static Result index() {
		return ok(views.html.urls.index.render(crawlLog));
	}
	
	public static Result getURLInfo(String url) {
		List<CrawlLogEntry> entries = crawlLog.getEntriesForURL(url);
		for (CrawlLogEntry e : entries) {
			Logger.info("Crawl ID: " + e.getLogId());
		}
		return ok(views.html.urls.urlInfo.render(entries));
	}
	
	public static Result getCompressability() {
		int intervals = getQueryParamAsInt("intervals", 50);
		
		// All counts, in native resolution
		List<Tuple> tuples = crawlLog.getCompressabilityHistogram();
		Logger.info("Got histogram from DB (" + tuples.size() + " buckets in original resolution)");
		
		// Now resample...
		int ctr = 0;
		int step = tuples.size() / intervals;
		Logger.info("Resampling to " + intervals + " intervals (" + step  + " stepsize)");
		
		List<Point2D> histogram = new ArrayList<Point2D>();
		while (ctr < tuples.size()) {
			double bucketSec = ((double) ctr) / 1000;
			int aggregated = 0;
			for (int i=0; i<step; i++) {
				if (ctr < tuples.size())
					aggregated += tuples.get(ctr)._2;
				ctr++;
			}
			Logger.info("Resampled value: " + bucketSec + " -> " + aggregated);
			histogram.add(new Point2D.Double(bucketSec, aggregated));
		}
		
		return ok(Json.toJson(histogram));
	}

}
