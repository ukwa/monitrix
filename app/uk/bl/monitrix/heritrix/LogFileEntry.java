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
import java.util.Map;
import java.util.zip.Deflater;

import play.Logger;

import com.google.common.net.InternetDomainName;

import uk.bl.monitrix.model.Alert;
import uk.bl.monitrix.model.Alert.AlertType;
import uk.bl.monitrix.model.CrawlLogEntry;
import uk.bl.monitrix.model.VirusRecord;

/**
 * An in-memory implementation of {@link CrawlLogEntry}, for use with {@link SimpleLogfileReader}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class LogFileEntry extends CrawlLogEntry {
	
	// TODO make configurable via config file
	private static final int TOO_MANY_PATH_SEGMENTS_THRESHOLD = 16;
	
	private static final String MSG_MALFORMED_URL = "Malformed URL: ";
	private static final String MSG_TOO_MANY_PATH_SEGMENTS = "Too many path segments in URL: ";
	
	private static DateFormat ISO_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
	
	private static DateFormat RFC2550_FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	
	private String logPath;
	
	private String line;
	
	private List<String> fields = new ArrayList<String>();
	
	private String bufferedHost = null;
	
	private String bufferedDomain = null;
	
	private String bufferedSubdomain = null;
	
	private Double bufferedCompressability = null;
	
	private List<Alert> alerts = new ArrayList<Alert>();
	
	private boolean parseFailed;
	
	public LogFileEntry(String logPath, String line) {
		init(logPath, line);
	}
	
	/**
	 * Package-private constructor and init method that can be used to re-use an instance of this object
	 * thus reducing GC activity.
	 */
	LogFileEntry() {
	}

	
	void init(String logPath, String line) {
		this.logPath = logPath;
		this.line = line;

		String[] split = line.split(" ");
		if( split.length < 11 ) {
			this.parseFailed = true;
			Logger.error("Got a split of length: "+split.length);
		} else {
			this.parseFailed = false;
		}

		// Column 1 - 11
		int ctr = 0;
		fields.clear();
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
		
		alerts.clear();		
		for (Alert alert : validate())
			alerts.add(alert);

		// Reset buffers
		bufferedHost = null;
		bufferedSubdomain = null;
		bufferedCompressability = null;
	}
	
	public boolean getParseFailed() {
		return this.parseFailed;
	}
	
	private List<Alert> validate() {
		List<Alert> alerts = new ArrayList<Alert>();
		
		double compressability = getCompressability();

		// TODO find the right threshold + make configurable
		if (compressability < 0.02)
			alerts.add(new DefaultAlert(this.getLogTimestamp().getTime(), this.getHost(), AlertType.COMPRESSABILITY, getURL()));
		
		String[] pathSegments = getURL().split("/");
		if ((pathSegments.length - 1) > TOO_MANY_PATH_SEGMENTS_THRESHOLD)
			alerts.add(new DefaultAlert(this.getLogTimestamp().getTime(), this.getHost(), AlertType.TOO_MANY_PATH_SEGMENTS, MSG_TOO_MANY_PATH_SEGMENTS + this.getURL()));

		return alerts;
	}
	
	private void parseHost() {
		HostParseResult result = LogFileEntry.extractDomainNames(this);
		this.bufferedHost = result.host;
		this.bufferedDomain = result.domain;
		this.bufferedSubdomain = result.subdomain;
		if (result.alert != null)
			alerts.add(result.alert);
	}
	
	public List<Alert> getAlerts() {
		return alerts;
	}
	
	@Override
	public String getLogId() {
		return logPath;
	}
	
	@Override
	public Date getLogTimestamp() {
		try {
			return ISO_FORMAT.parse(fields.get(0).replaceAll("Z$", "+0000"));
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
	public long getDownloadSize() {
		if (fields.get(2).equals("-"))
			return 0;
		
		return Long.parseLong(fields.get(2));
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
	public String getDomain() {
		if (bufferedDomain == null)
			parseHost();
		
		return bufferedDomain;
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
	public String getWorkerThread() {
		return fields.get(7);
	}
	
	@Override
	public Date getFetchTimestamp() {
		try {
			String timestamp = fields.get(8);
			if ("-".equals(timestamp)) return null;
			if (timestamp.indexOf('+') > -1)
				timestamp = timestamp.substring(0, timestamp.indexOf('+'));
			
			//Logger.info("fetch timestamp: " + timestamp);
			return RFC2550_FORMAT.parse(timestamp);
		} catch (ParseException e) {
			Logger.error("Bad date in line: "+this.line);
			// Should never happen!
			throw new RuntimeException(e);
		}
	}

	@Override
	public int getFetchDuration() {
		String duration = fields.get(8);
		if (duration.indexOf('+') > -1) {
			duration = duration.substring(duration.indexOf('+') + 1);
			return Integer.parseInt(duration);
		}
		
		return 0;
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
	public int getRetries() {
		for (String a : fields.get(11).split(",")) {
			if (a.endsWith("t")) {
				String retries = a.substring(0, a.length() - 1);
				try {
					return Integer.parseInt(retries);
				} catch (Throwable t) {
					// Do nothing
				}
			}
		}
		return 0;
	}
	

	// TODO Maybe switch to Snappy: http://xerial.org/snappy-java/
	Deflater compresser = new Deflater(Deflater.BEST_SPEED);
	
	@Override
	public double getCompressability() {
		if (bufferedCompressability == null) {
		  try {
			String url = getURL();
			if( url == null ) {
				Logger.error("Got URL == null from line: '"+line+"'");
				return 1.0;
			}
			try {
				// Get the input as bytes:
				byte[] input = url.getBytes("UTF-8");
				// Compress the bytes and get compressed length:
				byte[] output = new byte[input.length+100];
				compresser.setInput(input);
				compresser.finish();
				int compressedDataLength = compresser.deflate(output);
				//compresser.end();
				compresser.reset();

				// The smaller this ratio is, the more 'compressible' the string,
				// i.e. the more repetitive the URL
				bufferedCompressability = ((double) compressedDataLength) / ((double) input.length);
				
			} catch (IOException e) {
				Logger.error("Could not analyse URL for compressability: " + url);
			}
		  } catch ( Exception e ) {
			  Logger.error("Caught exception '"+e+"' when reading this URL from crawl log line: '"+ line + "'");
		  }
		}
		
		return bufferedCompressability;
	}
	
	/**
	 * Attempt to make sure the compressor gets cleaned up properly on GC:
	 */
	protected void finalize() {
		compresser.end();
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
		String domain = null;
		String subdomain = "";
		try {
			host = new URL(url).getHost();
			domain = InternetDomainName.from(host).topPrivateDomain().name();
			if (!domain.equals(host))
				subdomain = host.substring(0, host.lastIndexOf(domain) - 1);
			
			return new HostParseResult(host, domain, subdomain, null);
		} catch (MalformedURLException e) {
			// Logger.warn(e.getMessage());
			return new HostParseResult(url, url, subdomain, new DefaultAlert(entry.getLogTimestamp().getTime(), url, AlertType.MALFORMED_CRAWL_URL, MSG_MALFORMED_URL + url));
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
				domain = hostBuilder.toString().substring(1);
			}

			return new HostParseResult(host, domain, subdomain, new DefaultAlert(entry.getLogTimestamp().getTime(), host, AlertType.MALFORMED_CRAWL_URL, MSG_MALFORMED_URL + url));
		} catch (IllegalStateException e) {
			// Will be thrown by InternetDomainName.from in case the host name looks weird
			Logger.warn(e.getMessage());
			return new HostParseResult(host, domain, subdomain, new DefaultAlert(entry.getLogTimestamp().getTime(), url, AlertType.MALFORMED_CRAWL_URL, MSG_MALFORMED_URL + url));
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
		
		private String domain;
		
		private String subdomain;
		
		private Alert alert;
		
		HostParseResult(String host, String domain, String subdomain, Alert alert) {
			this.host = host;
			this.domain = domain;
			this.subdomain = subdomain;
			this.alert = alert;
		}
		
	}
	
	/**
	 * An in-memory implementation of {@link Alert}.
	 */
	public static class DefaultAlert implements Alert {
		
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
	
	/**
	 * An in-memory representation of a {@link VirusRecord}.
	 */
	public static class DefaultVirusRecord implements VirusRecord {
		
		private String name;
		private Map<String, Integer> occurences;

		public DefaultVirusRecord(String name, Map<String, Integer> occurences ) {
			this.name = name;
			this.occurences = occurences;
		}

		@Override
		public String getName() {
			return this.name;
		}

		@Override
		public Map<String, Integer> getOccurences() {
			return this.occurences;
		}
		
	}

}