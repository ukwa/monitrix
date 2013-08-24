package uk.bl.monitrix.model;

import java.util.Date;

/**
 * Represents a single line in  a Heritrix log.
 * Cf. http://crawler.archive.org/articles/user_manual/analysis.html
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public abstract class CrawlLogEntry {
	
	public static final String ANNOTATION_CAPPED_CRAWL = "Q:serverMaxSuccessKb";
	public static final String ANNOTATION_WARC_REVISIT = "warcRevisit";
	public static final String ANNOTATION_WARC_REVISIT_DIGEST = "warcRevisit:digest";
	public static final String ANNOTATION_WARC_REVISIT_NOT_MODIFIED = "warcRevisit:notModified";
	
	/**
	 * The ID of the log this entry is from.
	 * @return the log file path
	 */
	public abstract String getLogId();
	
	/**
	 * Column #1 - ISO timestamp.
	 * @return the crawl time
	 */
	public abstract Date getLogTimestamp();
	
	/**
	 * Column #2 - the HTTP status or Heritrix error code ('fetch status code').
	 * @return the HTTP status or error code
	 */
	public abstract int getHTTPCode();
	
	/**
	 * Column #3 - the download file size in bytes.
	 * @return the filesize
	 */
	public abstract long getDownloadSize();
	
	/**
	 * Column #4 - the URL of the crawled document.
	 * @return the URL
	 */
	public abstract String getURL();
	
	/**
	 * The hostname, extracted from the URL.
	 * @return the hostname
	 */
	public abstract String getHost(); 

	/**
	 * The domain, extracted from the host name.
	 * @return the domain
	 */
	public abstract String getDomain();
	
	/**
	 * The subdomain, extracted from the host name.
	 * @return the subdomain
	 */
	public abstract String getSubdomain();
	
	/**
	 * Column #5 - 'breadcrumb codes' showing the trail of downloads that lead to the 
	 * current URL ('discovery path'). The discovery path of a seed is empty.
	 * Cf. http://crawler.archive.org/articles/user_manual/glossary.html#discoverypath
	 * 
	 * @return
	 */
	public abstract String getBreadcrumbCodes();
	
	/**
	 * <code>true</code> if the URL is a seed (i.e. its discovery path is an empty string).
	 * @return
	 */
	public boolean isSeed() {
		return getBreadcrumbCodes().isEmpty();
	}
	
	/**
	 * Column #6 - the URL that immediately referenced this URL ('referrer'). The 
	 * referrer will be empty if the URL is a seed.
	 * @return the referrer URL
	 */
	public abstract String getReferrer();
	
	/**
	 * Column #7 - content type.
	 * @return the content type
	 */
	public abstract String getContentType();
	
	/**
	 * Column #8 - worker thread ID.
	 * @return the worker thread ID
	 */
	public abstract String getWorkerThread();
	
	/**
	 * The fetch timestamp, derived from column #8.
	 * @return the fetch timestamp
	 */
	public abstract Date getFetchTimestamp();
	
	/**
	 * The fetch timestamp, derived from column #8;
	 * @return
	 */
	public abstract int getFetchDuration();
	
	/**
	 * Column #10 - file SHA1 hash.
	 * @return the file hash
	 */
	public abstract String getSHA1Hash();
		
	/**
	 * Column #12 - annotations (comma-separated)
	 * @return the annotations
	 */
	public abstract String getAnnotations();
	
	/**
	 * Returns the number of HTTP retries (based on info in column #12)
	 * @return the number of retries
	 */
	public abstract int getRetries();
	
	/**
	 * Returns the 'compressability' of the URL
	 * @return the compressability
	 */
	public abstract double getCompressability();
	

	/**
	 * Determines whether this is a revisit record.
	 * @return
	 */
	public boolean isRevisitRecord() {
		for( String anno : getAnnotations().split(",") ) {
			if( anno.startsWith(ANNOTATION_WARC_REVISIT) ) return true;
		}
		return false;		
	}

	/**
	 * Determines is this is a capped-crawl record.
	 * @return
	 */
	public boolean isCappedCrawlRecord() {
		for( String anno : getAnnotations().split(",") ) {
			if( anno.startsWith(ANNOTATION_CAPPED_CRAWL)) return true;
		}
		return false;
	}
	
}
