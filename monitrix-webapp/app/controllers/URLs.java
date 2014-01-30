package controllers;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

import play.libs.Json;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.CrawlLogEntry;

public class URLs extends AbstractController {
	
	private static CrawlLog crawlLog  = Global.getBackend().getCrawlLog();
	
	public static Result index() {
		return ok(views.html.urls.index.render(crawlLog));
	}
	
	public static Result getURLInfo(String url) {
		List<CrawlLogEntry> entries = crawlLog.getEntriesForURL(url);
		return ok(views.html.urls.urlInfo.render(entries));
	}
	
	public static Result getCompressability() {
		int intervals = getQueryParamAsInt("intervals", 50);
		double increment = 2.0 / intervals;
		
		List<Point2D> histogram = new ArrayList<Point2D>();
		for (int i=0; i<intervals; i++) {
			double from = i * increment;
			double to = from + increment;
			histogram.add(new Point2D.Double(from, crawlLog.searchByCompressability(from, to, 0, 0).totalResults()));
		}
		
		return ok(Json.toJson(histogram));
	}

}
