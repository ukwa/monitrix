package controllers;

import controllers.mapping.IngestStatusMapper;

import play.libs.Json;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.heritrix.ingest.IngestWatcher;
import uk.bl.monitrix.model.CrawlLog;

public class Admin extends AbstractController{

	private static CrawlLog crawlLog = Global.getBackend().getCrawlLog();
	
	private static IngestWatcher ingestWatcher = Global.getIngestWatcher();
	
	public static Result index() {
		return ok(views.html.admin.index.render());
	}
	
	public static Result addLog() {
		String path = getFormParam("path");
		ingestWatcher.addLog(path);
		return redirect(routes.Admin.index());
	}
	
	public static Result getLogTrackerStatus() {
		return ok(Json.toJson(IngestStatusMapper.map(ingestWatcher.getStatus(), crawlLog)));
	}
	
}
