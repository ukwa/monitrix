package controllers;

import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.export.VirusReport;
import uk.bl.monitrix.model.VirusLog;

public class Viruses extends Controller {
	
	private static final String MIME_PDF = "application/pdf";
	
	private static VirusLog virusLog = Global.getBackend().getVirusLog();
	
	public static Result index() {
		return ok(views.html.viruses.index.render(virusLog));
	}
	
	public static Result pdf() {
		response().setContentType(MIME_PDF);
		VirusReport report = new VirusReport(virusLog);
		return ok(report.toPDF());
	}

}
