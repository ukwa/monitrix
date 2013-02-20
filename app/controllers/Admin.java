package controllers;

import java.io.File;

import controllers.mapping.IngestStatusMapper;

import play.Logger;
import play.libs.Json;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.heritrix.ingest.IngestWatcher;
import uk.bl.monitrix.model.IngestSchedule;

public class Admin extends AbstractController{
	
	private static final String ID = "id";
	private static final String PATH = "path";
	private static final String CRAWLER_ID = "crawler_id";

	private static IngestSchedule ingestSchedule = Global.getBackend().getIngestSchedule(); 
		
	private static IngestWatcher ingestWatcher = Global.getIngestWatcher();
	
	public static Result index() {
		return ok(views.html.admin.index.render());
	}
	
	public static Result addLog() {
		String crawlerId = getFormParam(CRAWLER_ID);
		if (crawlerId.isEmpty()) {
			flash("error", "Crawler ID may not be empty");
			return redirect(routes.Admin.index());
		}
		
		String path = getFormParam(PATH);
		if (path.isEmpty()) {
			flash("error", "Log file path may not be empty");
			return redirect(routes.Admin.index());
		}
		
		File file = new File(path);
		if (!file.exists()) {
			Logger.info("Attempt to add non-existing log file: " + path);
			flash("error", "The file '" + path + "' does not exist");
			return redirect(routes.Admin.index());
		}
		
		ingestSchedule.addLog(path, crawlerId, true);
		ingestWatcher.refresh();
		return redirect(routes.Admin.index());
	}
	
	public static Result getLogTrackerStatus() {
		if (ingestWatcher == null)
			return ok();
		
		return ok(Json.toJson(IngestStatusMapper.map(ingestWatcher.getStatus(), ingestSchedule)));
	}
	
	public static Result toggleWatch() {
		String logId = getQueryParam(ID);
		if (!logId.isEmpty())
			ingestSchedule.setMonitoringEnabled(logId, !ingestSchedule.isMonitoringEnabled(logId));

		return redirect(routes.Admin.index());
	}
	
}
