package uk.bl.monitrix;

import java.util.List;

public interface HostInformation {
	
	public String getHostname();
	
	public long getLastAccess();
	
	public long countCrawledURLs();
	
	public List<String> getCrawlers();
	
	public List<PieChartValue> getHTTPCodes();

}
