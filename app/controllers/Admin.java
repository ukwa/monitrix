package controllers;

import java.io.File;

import controllers.mapping.IngestStatusMapper;

import play.Logger;
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
		if (path.isEmpty())
			return redirect(routes.Admin.index());
		
		File file = new File(path);
		if (!file.exists()) {
			Logger.info("Attempt to add non-existing log file: " + path);
			flash("error", "The file '" + path + "' does not exist");
			return redirect(routes.Admin.index());
		}
		
		ingestWatcher.addLog(path);
		return redirect(routes.Admin.index());
	}
	
	public static Result getLogTrackerStatus() {
		if (ingestWatcher == null)
			return ok();
		
		return ok(Json.toJson(IngestStatusMapper.map(ingestWatcher.getStatus(), crawlLog)));
	}
	
}
