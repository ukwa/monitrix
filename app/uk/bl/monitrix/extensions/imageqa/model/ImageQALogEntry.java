package uk.bl.monitrix.extensions.imageqa.model;

public interface ImageQALogEntry {
	
	/**
	 * The URL of the original Web page.
	 * @return the Web page URL
	 */
	public String getOriginalWebURL();
	
	/**
	 * The URL of the archived Web page on Wayback.
	 * @return the Wayback page URL
	 */
	public String getWaybackImageURL();
	
	/**
	 * The URL of the screenshot image of the original page.
	 * @return the original Web page screenshot URL
	 */
	public String getOriginalImageURL();
	
	/**
	 * The message produced as a result of the (SIFT-based?) image comparison.
	 * @return the message produced by the (SIFT-based?) image comparison
	 */
	public String getMessage();
	
	/**
	 * The message produced as a result of the (pixel-based?) PSNR image comparison.
	 * @return the message produced by the (pixel-based?) PSNR image comparison
	 */
	public String getPSNRMessage();

}
