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
	private static final String MIN = "min";
	private static final String MAX = "max";
	
	private static final String TYPE_TLD = "tld";
	private static final String TYPE_URL = "url";
	private static final String TYPE_HOST = "hostname";
	private static final String TYPE_ANNOTATION = "annotation";
	private static final String TYPE_AVG_FETCH_DURATION = "fetch_duration";
	private static final String TYPE_COMPRESSABILITY = "compressability";
	private static final String TYPE_AVG_RETRIES = "avg_retries";
	
	private static DBConnector db = Global.getBackend();
	
	private static CrawlLog crawlLog = db.getCrawlLog();
	
	private static KnownHostList knownHostList = db.getKnownHostList();
	
	public static Result search() {
		String query = getQueryParam(QUERY);
		String type = getQueryParam(TYPE);
		
		int limit = getQueryParamAsInt(LIMIT, 20);
		int offset = getQueryParamAsInt(OFFSET, 0);
		
		if (type == null && query == null)
			return ok(views.html.search.advanced.render());
		
		if (type == null && query != null) {
			// Default to combined host + URL search
			SearchResult hosts = knownHostList.searchHosts(query, limit, offset);	
			long urlPreview = crawlLog.searchByURL(query, 1, 0).totalResults();
		
			if (hosts.totalResults() == 0 && urlPreview > 0) {
				SearchResult urls = crawlLog.searchByURL(query, limit, offset);
				return ok(views.html.search.urlResults.render(urls, hosts.totalResults(), null, null));
			} else {
				return ok(views.html.search.hostResults.render(hosts, urlPreview, null, null));
			}
		}
		
		if (type.equalsIgnoreCase(TYPE_TLD) && query != null) {
			return ok(views.html.search.hostResults.render(knownHostList.searchByTopLevelDomain(query, limit, offset), null, TYPE_TLD, null));
			
		} else if (type.equalsIgnoreCase(TYPE_URL) && query != null) {
			SearchResult urls = crawlLog.searchByURL(query, limit, offset);
			return ok(views.html.search.urlResults.render(urls, null, TYPE_URL, null));
			
		} else if (type.equalsIgnoreCase(TYPE_HOST) && query != null) {
			SearchResult hosts = knownHostList.searchHosts(query, limit, offset);	
			return ok(views.html.search.hostResults.render(hosts, null, TYPE_HOST, null));
			
		} else if (type.equalsIgnoreCase(TYPE_ANNOTATION) && query != null) {
			SearchResult urls = crawlLog.searchByAnnotation(query, limit, offset);
			return ok(views.html.search.urlResults.render(urls, null, TYPE_ANNOTATION, null));
			
		} else if (type.equalsIgnoreCase(TYPE_AVG_FETCH_DURATION)) {
			int min = getQueryParamAsInt(MIN, -1);
			int max = getQueryParamAsInt(MAX, -1);
			
			if (min > -1 && max > min) {
				SearchResult hosts = knownHostList.searchByAverageFetchDuration(min, max, limit, offset);
				return ok(views.html.search.hostResults.render(hosts, null, TYPE_AVG_FETCH_DURATION, "&min=" + min + "&max=" + max));
			}
		} else if (type.equalsIgnoreCase(TYPE_COMPRESSABILITY)) {
			double min = getQueryParamAsDouble(MIN, -1);
			double max = getQueryParamAsDouble(MAX, -1);
			
			if (min > -1 && max > min) {
				SearchResult urls = crawlLog.searchByCompressability(min, max, limit, offset);
				return ok(views.html.search.urlResults.render(urls, null, TYPE_COMPRESSABILITY, "&min=" + min + "&max=" + max));
			}
		} else if (type.equalsIgnoreCase(TYPE_AVG_RETRIES)) {
			int min = getQueryParamAsInt(MIN, -1);
			int max = getQueryParamAsInt(MAX, -1);
			
			if (min > -1 && max > min) {
				SearchResult hosts = knownHostList.searchByAverageRetries(min, max, limit, offset);
				return ok(views.html.search.hostResults.render(hosts, null, TYPE_AVG_RETRIES, "&min=" + min + "&max=" + max));				
			}
		}
		
		// Only happens if someone messes with the query string manually
		return ok(views.html.search.advanced.render());
	}
	
}
