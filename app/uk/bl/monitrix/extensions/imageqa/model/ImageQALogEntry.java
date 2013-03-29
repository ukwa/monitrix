package uk.bl.monitrix.extensions.imageqa.model;

import java.util.Date;

/**
 * An interface for entries in Roman Graf's image quality assurance logs.
 * Entries are formatted according to the following convetion:
 * 
 *  { "current_time": current_time,
 *    "execution_time": execution_time,
 *    "orig_web_url": orig_link,
 *    "wayback_image": wayback_link,
 *    "wayback_timestamp": wayback_timestamp,
 *    "fc1": fc1,
 *    "fc2": fc2,
 *    "mc": mc,
 *    "msg": msg,
 *    "ts1": ts1,
 *    "ts2": ts2,
 *    "ocr": ocr,
 *    "img1_size": img1_size,
 *    "img2_size": img2_size,
 *    "psnr_similarity": psnr_similarity,
 *    "psnr_threshold": psnr_threshold,
 *    "psnr_msg": psnr_msg,
 *    "orig_image": orig_link_path }
 *    
 * Example log line (with line breaks inserted at ';' where appropriate):
 * 
 * 2013-03-21 16:38:01.850688;1.07782793045;http://www.acses.org.uk/;
 * http://www.webarchive.org.uk/thumbs/66158797/66128255c.jpg;66158797;
 * 1681;52;252;VERY DIFFERENT;60;30;0;438699;21483;2.6929;3.0;
 * DIFFERENT;http://127.0.0.1:8000/qa/orig/1363883878.13.png;
 *  
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public interface ImageQALogEntry {
	
	/**
	 * The time of the QA execution.
	 * @return the time of execution
	 */
	public Date getTimestamp();
	
	/**
	 * The *duration* the execution took (in seconds?)
	 * @return the duration of the execution
	 */
	public double getExecutionTime();
	
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
	
	public long getWaybackTimestamp();
	
	public int getFC1();
	
	public int getFC2();
	
	public int getMC();
	
	/**
	 * The message produced as a result of the (SIFT-based?) image comparison.
	 * @return the message produced by the (SIFT-based?) image comparison
	 */
	public String getMessage();

	public int getTS1();
	
	public int getTS2();
	
	public int getOCR();
	
	public int getImage1Size();
	
	public int getImage2Size();
	
	public double getPSNRSimilarity();
	
	public double getPSNRThreshold();
	
	/**
	 * The message produced as a result of the (pixel-based?) PSNR image comparison.
	 * @return the message produced by the (pixel-based?) PSNR image comparison
	 */
	public String getPSNRMessage();

	/**
	 * The URL of the screenshot image of the original page.
	 * @return the original Web page screenshot URL
	 */
	public String getOriginalImageURL();

}
