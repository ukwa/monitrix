package controllers;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import controllers.mapping.HeritrixSummaryMapper;

import play.libs.Akka;
import play.libs.F.Function;
import play.libs.F.Promise;
import play.libs.Json;
import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.heritrix.api.HeritrixAPI;
import uk.bl.monitrix.heritrix.api.HeritrixSummary;

public class Heritrix extends AbstractController {
	
	private static final String CRAWLER_URL = "heritrix_url";
	private static final String HTTP_USER = "heritrix_username";
	private static final String HTTP_PASSWORD = "heritrix_password";
	
	private static List<HeritrixAPI> crawlers = Global.getCrawlerAPIs();
	
	public static Result index() {
		return ok(views.html.heritrix.index.render(crawlers));
	}
	
	public static Result addCrawler() {
		String sUrl = getFormParam(CRAWLER_URL);
		if (sUrl.isEmpty()) {
			flash("error", "Crawler Endpoint URL may not be empty");
			return redirect(routes.Heritrix.index());
		}
		
		URL url = null;
		try {
			url = new URL(sUrl);
		} catch (MalformedURLException e) {
			flash("error", "Invalid Endpoint URL: " + sUrl);
			return redirect(routes.Heritrix.index());			
		}
		
		String authUser = getFormParam(HTTP_USER);
		String authPassword = getFormParam(HTTP_PASSWORD);
		
		crawlers.add(new HeritrixAPI(url, authUser, authPassword));
		return redirect(routes.Heritrix.index());
	}
	
	public static Result getCrawlersJSON() {
		// Where's my scala .map? ;-)
		List<String> urls = new ArrayList<String>();
		for (HeritrixAPI api : crawlers)
			urls.add(api.getEndpointURL());
		
		return ok(Json.toJson(urls));
	}
	
	public static Result getCrawlerSummaryJSON(String url) {
		final HeritrixAPI api = get(url);
		if (api == null)
			return notFound();

		Promise<HeritrixSummary> promise = Akka.future(new Callable<HeritrixSummary>() {
			@Override
			public HeritrixSummary call() throws Exception {
				return api.getSummary();
			}
		});
		
		return async(promise.map(new Function<HeritrixSummary, Result>() {
			@Override
			public Result apply(HeritrixSummary summary) throws Throwable {
				return ok(Json.toJson(new HeritrixSummaryMapper(summary)));
			}
		}));
	}
	
	private static HeritrixAPI get(String url) {
		// This is in need of optimization
		for (HeritrixAPI api : crawlers)
			if (api.getEndpointURL().equals(url))
				return api;
		
		return null;
	}

}
