package uk.bl.monitrix.heritrix;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import play.Logger;

import com.google.common.net.InternetDomainName;

import uk.bl.monitrix.model.Alert;
import uk.bl.monitrix.model.Alert.AlertType;
import uk.bl.monitrix.model.CrawlLogEntry;

/**
 * An in-memory implementation of {@link CrawlLogEntry}, for use with {@link LogfileReader}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class LogFileEntry extends CrawlLogEntry {
	
	// TODO make configurable via config file
	private static final int TOO_MANY_PATH_SEGMENTS_THRESHOLD = 16;
	
	private static final String MSG_MALFORMED_URL = "Malformed URL: ";
	private static final String MSG_TOO_MANY_PATH_SEGMENTS = "Too many path segments in URL: ";
	
	private static DateFormat ISO_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	
	private String line;
	
	private List<String> fields = new ArrayList<String>();
	
	private String bufferedHost = null;
	
	private List<Alert> alerts = new ArrayList<Alert>();
	
	public LogFileEntry(String line) {
		this.line = line;

		String[] split = line.split(" ");

		// Column 1 - 11
		int ctr = 0;
		while (fields.size() < 11 && ctr < split.length) {
			if (!split[ctr].isEmpty())
				fields.add(split[ctr].trim());
			ctr++;
		}
		
		// Column 12 (annotations) - note that annotations may contain white spaces, so we need to re-join
		StringBuilder sb = new StringBuilder();
		for (int i=ctr; i<split.length; i++) {
			sb.append(split[i] + " ");
		}
		
		fields.add(sb.toString().trim());
		
		for (Alert alert : validate())
			alerts.add(alert);
	}
	
	private List<Alert> validate() {
		List<Alert> alerts = new ArrayList<Alert>();
		
		String[] pathSegments = this.getURL().split("/");
		if ((pathSegments.length - 1) > TOO_MANY_PATH_SEGMENTS_THRESHOLD)
			alerts.add(new DefaultAlert(this.getTimestamp().getTime(), this.getHost(), AlertType.TOO_MANY_PATH_SEGMENTS, MSG_TOO_MANY_PATH_SEGMENTS + this.getURL()));

		return alerts;
	}
	
	public List<Alert> getAlerts() {
		return alerts;
	}
	
	@Override
	public Date getTimestamp() {
		try {
			return ISO_FORMAT.parse(fields.get(0));
		} catch (ParseException e) {
			// Should never happen!
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public int getHTTPCode() {
		return Integer.parseInt(fields.get(1));
	}
	
	@Override
	public int getDownloadSize() {
		if (fields.get(2).equals("-"))
			return 0;
		
		return Integer.parseInt(fields.get(2));
	}
	
	@Override
	public String getURL() {
		return fields.get(3);
	}
	
	@Override
	public String getHost() {
		if (bufferedHost == null) {
			HostParseResult parseResult = getHost(this);
			bufferedHost = parseResult.hostname;
			if (parseResult.alert != null)
				alerts.add(parseResult.alert);
		}
		
		return bufferedHost;
	}
	
	@Override
	public String getBreadcrumbCodes() {
		return fields.get(4);
	}

	@Override
	public String getReferrer() {
		return fields.get(5);
	}
	
	@Override
	public String getContentType() {
		return fields.get(6);
	}
	
	@Override
	public String getCrawlerID() {
		return fields.get(7);
	}

	@Override
	public String getSHA1Hash() {
		return fields.get(9);
	}
	
	@Override
	public String getAnnotations() {
		return fields.get(11);
	}
	
	@Override
	public String toString() {
		return line;
	}
	
	/**
	 * Helper method to extract the domain name from a URL. 
	 * Cf. http://stackoverflow.com/questions/4819775/implementing-public-suffix-extraction-using-java
	 * @param url the URL
	 * @return the domain name
	 */
	private static HostParseResult getHost(LogFileEntry entry) {
		// Not the nicest solution - but neither java.net.URL nor com.google.common.net.InternetDomainName
		// can handle Heritrix' custom 'dns:' protocol prefix.
		String url = entry.getURL();
		if (url.startsWith("dns:"))
			url = "http://" + url.substring(4);
		
		String host = null;
		try {
			host = new URL(url).getHost();
			InternetDomainName domainName = InternetDomainName.from(host);
			return new HostParseResult(domainName.topPrivateDomain().name(), null);
		} catch (MalformedURLException e) {
			Logger.warn(e.getMessage());
			return new HostParseResult(url, new DefaultAlert(entry.getTimestamp().getTime(), url, AlertType.MALFORMED_CRAWL_URL, MSG_MALFORMED_URL + url));
		} catch (IllegalArgumentException e) {
			// Will be thrown by InternetDomainName.from in case the host name looks weird
			Logger.warn(e.getMessage());
			
			// Special handling for the most common error cause - subdomains ending with '-'
			String[] tokens = host.split(".");
			int offendingToken = 0;
			for (int i=0; i<tokens.length; i++) {
				if (tokens[i].endsWith("-"))
					offendingToken = i;
			}
			
			if (offendingToken > 0) {
				StringBuilder sb = new StringBuilder();
				for (int i=offendingToken + 1; i<tokens.length; i++)
					sb.append(tokens[i]);
				host = sb.toString();
			}
			
			return new HostParseResult(host, new DefaultAlert(entry.getTimestamp().getTime(), host, AlertType.MALFORMED_CRAWL_URL, MSG_MALFORMED_URL + url));
		}
	}
	
	/**
	 * Simple helper class to wrap the result of host-from-URL parsing.
	 */
	private static class HostParseResult {
		public String hostname;
		public DefaultAlert alert;
		
		HostParseResult(String hostname, DefaultAlert alert) {
			this.hostname = hostname;
			this.alert = alert;
		}
	}
	
	/**
	 * An in-memory implementation of {@link Alert}.
	 */
	private static class DefaultAlert implements Alert {
		
		private long timestamp;
		
		private String offendingHost;
		
		private AlertType type;
		
		private String description;
		
		public DefaultAlert(long timestamp, String offendingHost, AlertType type, String description) {
			this.timestamp = timestamp;
			this.offendingHost = offendingHost;
			this.type = type;
			this.description = description;
		}
		
		@Override
		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public String getOffendingHost() {
			return offendingHost;
		}

		@Override
		public AlertType getAlertType() {
			return type;
		}

		@Override
		public String getAlertDescription() {
			return description;
		}

	}

}