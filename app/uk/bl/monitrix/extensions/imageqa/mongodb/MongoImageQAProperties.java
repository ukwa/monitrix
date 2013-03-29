package uk.bl.monitrix.extensions.imageqa.mongodb;

public class MongoImageQAProperties {
	
	/** Database collection name **/
	public static final String COLLECTION_IMAGE_QA_LOG = "image_qa_results";
	
	public static final String FIELD_IMAGE_QA_LOG_TIMESTAMP = "current_time";
	public static final String FIELD_IMAGE_QA_LOG_EXECUTION_TIME = "execution_time";
	public static final String FIELD_IMAGE_QA_LOG_ORIGINAL_WEB_URL  = "orig_web_url";
	public static final String FIELD_IMAGE_QA_LOG_WAYBACK_IMAGE_URL  = "wayback_image";
	public static final String FIELD_IMAGE_QA_LOG_WAYBACK_TIMESTAMP = "wayback_timestamp";
	public static final String FIELD_IMAGE_QA_LOG_FC1 = "fc1";
	public static final String FIELD_IMAGE_QA_LOG_FC2 = "fc2";
	public static final String FIELD_IMAGE_QA_LOG_MC = "mc";
	public static final String FIELD_IMAGE_QA_LOG_MESSAGE = "msg";
	public static final String FIELD_IMAGE_QA_LOG_TS1 = "ts1";
	public static final String FIELD_IMAGE_QA_LOG_TS2 = "ts2";
	public static final String FIELD_IMAGE_QA_LOG_OCR = "ocr";
	public static final String FIELD_IMAGE_QA_LOG_IMG1_SIZE = "img1_size";
	public static final String FIELD_IMAGE_QA_LOG_IMG2_SIZE = "img2_size";
	public static final String FIELD_IMAGE_QA_LOG_PSNR_SIMILARITY = "psnr_similarity";
	public static final String FIELD_IMAGE_QA_LOG_PSNR_THRESHOLD = "psnr_threshold";
	public static final String FIELD_IMAGE_QA_LOG_PSNR_MESSAGE = "psnr_msg";
	public static final String FIELD_IMAGE_QA_LOG_ORIGINAL_IMAGE_URL = "orig_image";

}
