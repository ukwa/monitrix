package controllers;

import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.model.VirusLog;

public class Viruses extends Controller {
	
	private static VirusLog virusLog = Global.getBackend().getVirusLog();
	
	public static Result index() {
		return ok(views.html.viruses.index.render(virusLog));
	}

}
