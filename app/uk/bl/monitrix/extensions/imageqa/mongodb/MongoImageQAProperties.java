package uk.bl.monitrix.extensions.imageqa.mongodb;

public class MongoImageQAProperties {
	
	/** Database collection name **/
	public static final String COLLECTION_IMAGE_QA_LOG = "image_qa_results";
	
	public static final String FIELD_IMAGE_QA_LOG_ORIGINAL_WEB_URL  = "orig_web_url";
	public static final String FIELD_IMAGE_QA_LOG_WAYBACK_IMAGE_URL  = "wayback_image";
	public static final String FIELD_IMAGE_QA_LOG_ORIGINAL_IMAGE_URL = "orig_image";
	public static final String FIELD_IMAGE_QA_LOG_MESSAGE = "msg";
	public static final String FIELD_IMAGE_QA_LOG_PSNR_MESSAGE = "psnr_msg";
	public static final String FIELD_IMAGE_QA_LOG_LINE = "line";

}
