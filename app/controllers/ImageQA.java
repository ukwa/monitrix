package controllers;

import play.mvc.Controller;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.extensions.imageqa.mongodb.MongoImageQAProperties;
import uk.bl.monitrix.extensions.imageqa.mongodb.model.MongoImageQALog;

public class ImageQA extends Controller {
	
	private static MongoImageQALog log = Global.getBackend().getExtensionTable(MongoImageQAProperties.COLLECTION_IMAGE_QA_LOG, MongoImageQALog.class);
	
	public static Result index() {
		return ok(views.html.imageqa.index.render(log));
	}

}
