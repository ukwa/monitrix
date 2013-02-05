package uk.bl.monitrix.heritrix;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;

import play.Logger;

import com.google.common.net.InternetDomainName;

import uk.bl.monitrix.model.Alert;
import uk.bl.monitrix.model.Alert.AlertType;
import uk.bl.monitrix.model.CrawlLogEntry;

/**
 * An in-memory implementation of {@link CrawlLogEntry}, for use with {@link SimpleLogfileReader}.
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
	
	private String bufferedSubdomain = null;
	
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
		
		String url = this.getURL();
		try {
			ByteArrayOutputStream b64os = new ByteArrayOutputStream();
			GZIPOutputStream gzip = new GZIPOutputStream(b64os);
			gzip.write(url.getBytes());
			gzip.flush();
			
			// The smaller this ratio is, the more 'compressible' the string,
			// i.e. the more repetitive the URL
			double ratio = ((double) b64os.toByteArray().length) / ((double) url.length());
			
			// TODO find the right threshold + make configurable
			if (ratio < 0.02)
				alerts.add(new DefaultAlert(this.getTimestamp().getTime(), this.getHost(), AlertType.COMPRESSABILITY, url));
			
			gzip.close();
			b64os.close();
		} catch (IOException e) {
			Logger.error("Could not analyse URL for compressability: " + url);
		}
		
		String[] pathSegments = url.split("/");
		if ((pathSegments.length - 1) > TOO_MANY_PATH_SEGMENTS_THRESHOLD)
			alerts.add(new DefaultAlert(this.getTimestamp().getTime(), this.getHost(), AlertType.TOO_MANY_PATH_SEGMENTS, MSG_TOO_MANY_PATH_SEGMENTS + this.getURL()));

		return alerts;
	}
	
	private void parseHost() {
		HostParseResult result = LogFileEntry.extractDomainNames(this);
		this.bufferedHost = result.host;
		this.bufferedSubdomain = result.subdomain;
		if (result.alert != null)
			alerts.add(result.alert);
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
		if (bufferedHost == null)
			parseHost();
		
		return bufferedHost;
	}
	
	@Override
	public String getSubdomain() {
		if (bufferedSubdomain == null)
			parseHost();
			
		return bufferedSubdomain;
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
	private static HostParseResult extractDomainNames(LogFileEntry entry) {
		// Not the nicest solution - but neither java.net.URL nor com.google.common.net.InternetDomainName
		// can handle Heritrix' custom 'dns:' protocol prefix.
		String url = entry.getURL();
		if (url.startsWith("dns:"))
			url = "http://" + url.substring(4);
		
		String host = null;
		String subdomain = "";
		try {
			host = new URL(url).getHost();
			String domainName = InternetDomainName.from(host).topPrivateDomain().name();
			if (!domainName.equals(host))
				subdomain = host.substring(0, host.lastIndexOf(domainName) - 1);
			
			return new HostParseResult(domainName, subdomain, null);
		} catch (MalformedURLException e) {
			// Logger.warn(e.getMessage());
			return new HostParseResult(url, subdomain, new DefaultAlert(entry.getTimestamp().getTime(), url, AlertType.MALFORMED_CRAWL_URL, MSG_MALFORMED_URL + url));
		} catch (IllegalArgumentException e) {
			// Will be thrown by InternetDomainName.from in case the host name looks weird
			// Logger.warn(e.getMessage());
			
			// Special handling for the most common error cause - subdomains ending with '-'
			String[] tokens = host.split("\\.");
			int offendingToken = -1;
			for (int i=0; i<tokens.length; i++) {
				if (tokens[i].endsWith("-"))
					offendingToken = i;
			}
			
			if (offendingToken > -1) {
				StringBuilder subdomainBuilder = new StringBuilder();
				for (int i=0; i<offendingToken + 1; i++)
					subdomainBuilder.append("." + tokens[i]);
				subdomain = subdomainBuilder.toString().substring(1);
				
				StringBuilder hostBuilder = new StringBuilder();
				for (int i=offendingToken + 1; i<tokens.length; i++)
					hostBuilder.append("." + tokens[i]);
				host = hostBuilder.toString().substring(1);
			}

			return new HostParseResult(host, subdomain, new DefaultAlert(entry.getTimestamp().getTime(), host, AlertType.MALFORMED_CRAWL_URL, MSG_MALFORMED_URL + url));
		} catch (IllegalStateException e) {
			// Will be thrown by InternetDomainName.from in case the host name looks weird
			Logger.warn(e.getMessage());
			return new HostParseResult(host, subdomain, new DefaultAlert(entry.getTimestamp().getTime(), url, AlertType.MALFORMED_CRAWL_URL, MSG_MALFORMED_URL + url));
		} catch (Throwable e) {
			Logger.warn("Offending host: " + host);
			Logger.warn("Extracted subdomain: " + subdomain);
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Simple helper class to wrap the result of host-from-URL parsing.
	 */
	private static class HostParseResult {
		
		private String host;
		
		private String subdomain;
		
		private Alert alert;
		
		HostParseResult(String host, String subdomain, Alert alert) {
			this.host = host;
			this.subdomain = subdomain;
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