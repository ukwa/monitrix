package uk.bl.monitrix.extensions.imageqa.mongodb;

public class ImageQAMongoProperties {
	
	/** Database collection name **/
	public static final String COLLECTION_IMAGE_QA_LOG = "image_qa_results";
	
	public static final String FIELD_IMAGE_QA_LOG_ORIGINAL_WEB_URL  = "original_web_url";
	public static final String FIELD_IMAGE_QA_LOG_WAYBACK_IMAGE_URL  = "wayback_image_url";
	public static final String FIELD_IMAGE_QA_LOG_ORIGINAL_IMAGE_URL = "original_image_url";
	public static final String FIELD_IMAGE_QA_LOG_MESSAGE = "message";
	public static final String FIELD_IMAGE_QA_LOG_PSNR_MESSAGE = "psnr_message";
	public static final String FIELD_IMAGE_QA_LOG_LINE = "line";

}
