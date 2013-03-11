package controllers;

import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.SearchResult;

public class Search extends AbstractController {

	// String constants
	private static final String QUERY = "query";
	private static final String LIMIT = "limit";
	private static final String OFFSET = "offset";
	
	private static DBConnector db = Global.getBackend();
	
	private static CrawlLog crawlLog = db.getCrawlLog();
	
	public static Result searchHosts() {
		String query = getQueryParam(QUERY);
		int limit = getQueryParamAsInt(LIMIT, 20);
		int offset = getQueryParamAsInt(OFFSET, 0);
		
		if (query == null) {
			// TODO error handling
			return notFound();
		} else {
			long urlPreview = crawlLog.searchURLs(query, 1, 0).totalResults();
			SearchResult hosts = db.getKnownHostList().searchHosts(query, limit, offset);
			
			if (hosts.totalResults() == 0 && urlPreview > 0)
				return redirect("/urls?query=" + query + "&limit=" + limit + "&offset=" + offset);
			
			return ok(views.html.search.hostResults.render(hosts, urlPreview));
		}
	}
	
	public static Result searchURLs() {
		String query = getQueryParam(QUERY);
		int limit = getQueryParamAsInt(LIMIT, 20);
		int offset = getQueryParamAsInt(OFFSET, 0);
		
		if (query == null) {
			// TODO error handling
			return notFound();
		} else {
			long hostPreview = db.getKnownHostList().searchHosts(query, 1, 0).totalResults();
			SearchResult urls = crawlLog.searchURLs(query, limit, offset);
			return ok(views.html.search.urlResults.render(urls, hostPreview));
		}
	}
	
}
