package controllers;

import java.util.HashMap;
import java.util.Map;

import controllers.mapping.LogTrackerStatusMapper;

import play.libs.Json;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.ingest.IngestorPool;
import uk.bl.monitrix.ingest.IngestorStatus;

public class Admin extends AbstractController{

	private static IngestorPool pool = Global.getIngestorPool();
	
	public static Result index() {
		return ok(views.html.admin.index.render());
	}
	
	public static Result addLog() {
		String path = getFormParam("path");
		pool.addHeritrixLog(path);
		return redirect(routes.Admin.index());
	}
	
	public static Result getLogTrackerStatus() {
		Map<String, IngestorStatus> trackerStatus = new HashMap<String, IngestorStatus>();
		for (String log : pool.getTrackedLogs()) {
			trackerStatus.put(log, pool.getStatus(log));
		}
		
		return ok(Json.toJson(LogTrackerStatusMapper.map(trackerStatus)));
	}
	
}
