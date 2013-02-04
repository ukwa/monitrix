package controllers;

import java.util.List;

import controllers.mapping.CrawlLogEntryMapper;
import controllers.mapping.CrawlStatsUnitMapper;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.model.AlertLog;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.CrawlStats;
import uk.bl.monitrix.model.CrawlStatsUnit;
import uk.bl.monitrix.model.KnownHostList;

public class Home extends Controller {
	
	private static DBConnector backend = Global.getBackend();
	
	private static CrawlLog log = backend.getCrawlLog();
	
	private static CrawlStats stats = backend.getCrawlStats();
	
	private static KnownHostList knownHosts = backend.getKnownHostList();
	
	private static AlertLog alerts = backend.getAlertLog();
	
	public static Result index() {
		return ok(views.html.home.index.render(log, stats, knownHosts, alerts));
	}
	
	// TODO not sure where to put this API method...
	public static Result getMostRecentLogEntries() {
		return ok(Json.toJson(CrawlLogEntryMapper.map(log.getMostRecentEntries(100))));
	}
	
	// TODO not sure where to put this API method...
	public static Result getMostRecentStats() {
		List<CrawlStatsUnit> mostRecent = stats.getMostRecentStats(2);
		if (mostRecent.size() < 2)
			return ok();
		return ok(Json.toJson(new CrawlStatsUnitMapper(mostRecent.get(1))));
	}
  
}