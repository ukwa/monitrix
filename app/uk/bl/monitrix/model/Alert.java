package uk.bl.monitrix.model;

/**
 * The Alert domain object interface. Alerts encapsulate various types of warnings
 * flagged up during the crawl.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface Alert {
	
	/**
	 * The timestamp at which the alert occured originally.
	 * @return the timestamp
	 */
	public long getTimestamp();

	/**
	 * The offending host that is the source of this alert.
	 * @return the hostname
	 */
	public String getOffendingHost();

	/**
	 * The alert type.
	 * @return the alert type
	 */
	public AlertType getAlertType();
	
	/**
	 * A description for the alert.
	 * TODO how to handle I18N? 
	 * @return the description
	 */
	public String getAlertDescription();
	
	/**
	 * The list of supported alert types.
	 */
	public enum AlertType {
		
		/** Something was wrong with the crawl URL **/
		MALFORMED_CRAWL_URL,
		
		/** A crawl URL with too many path segments **/
		TOO_MANY_PATH_SEGMENTS,
		
		/** A host has a suspiciously high number of subdomains **/
		TOO_MANY_SUBDOMAINS,
		
		/** A host serves a suspicsiously high rate of text/non-text MIME types - crawler block? **/
		TXT_TO_NONTEXT_RATIO,
		
		/** The URL is very 'compressable' which means there are likely many repetitions in it **/
		COMPRESSABILITY,
		
		/** The host has hit the crawl cap and no more will be crawled **/
		HOST_CAPPED
		
	}

}
