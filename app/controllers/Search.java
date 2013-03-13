package controllers;

import play.mvc.Result;
import uk.bl.monitrix.Global;
import uk.bl.monitrix.database.DBConnector;
import uk.bl.monitrix.model.CrawlLog;
import uk.bl.monitrix.model.KnownHostList;
import uk.bl.monitrix.model.SearchResult;

public class Search extends AbstractController {

	// String constants
	private static final String QUERY = "query";
	private static final String TYPE = "type";
	private static final String LIMIT = "limit";
	private static final String OFFSET = "offset";
	
	private static final String TYPE_TLD = "tld";
	private static final String TYPE_URL = "url";
	private static final String TYPE_HOST = "hostname";
	private static final String TYPE_ANNOTATION = "annotation";
	
	private static DBConnector db = Global.getBackend();
	
	private static CrawlLog crawlLog = db.getCrawlLog();
	
	private static KnownHostList knownHostList = db.getKnownHostList();
	
	public static Result search() {
		String query = getQueryParam(QUERY);
		String type = getQueryParam(TYPE);
		
		int limit = getQueryParamAsInt(LIMIT, 20);
		int offset = getQueryParamAsInt(OFFSET, 0);
		
		if (query == null)
			return ok(views.html.search.advanced.render());
		
		if (type == null) {
			// Default to combined host + URL search
			SearchResult hosts = knownHostList.searchHosts(query, limit, offset);	
			long urlPreview = crawlLog.searchByURL(query, 1, 0).totalResults();
		
			if (hosts.totalResults() == 0 && urlPreview > 0) {
				SearchResult urls = crawlLog.searchByURL(query, limit, offset);
				return ok(views.html.search.urlResults.render(urls, hosts.totalResults(), null));
			} else {
				return ok(views.html.search.hostResults.render(hosts, urlPreview, null));
			}
		}
		
		if (type.equalsIgnoreCase(TYPE_TLD)) {
			return ok(views.html.search.hostResults.render(knownHostList.searchByTopLevelDomain(query, limit, offset), null, TYPE_TLD));
		} else if (type.equalsIgnoreCase(TYPE_URL)) {
			SearchResult urls = crawlLog.searchByURL(query, limit, offset);
			return ok(views.html.search.urlResults.render(urls, null, TYPE_URL));
		} else if (type.equalsIgnoreCase(TYPE_HOST)) {
			SearchResult hosts = knownHostList.searchHosts(query, limit, offset);	
			return ok(views.html.search.hostResults.render(hosts, null, TYPE_HOST));
		} else if (type.equalsIgnoreCase(TYPE_ANNOTATION)) {
			SearchResult urls = crawlLog.searchByAnnotation(query, limit, offset);
			return ok(views.html.search.urlResults.render(urls, null, TYPE_ANNOTATION));
		}
		
		// Only happens if someone messes with the query string manually
		return ok(views.html.search.advanced.render());
	}
	
}