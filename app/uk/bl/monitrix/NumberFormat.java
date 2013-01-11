package uk.bl.monitrix;

/**
 * Various helper functions for number and date formatting.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class NumberFormat {
	
    private static final int MINUTE = 60000;
    private static final int HOUR = 60 * MINUTE;
    private static final int DAY = 24 * HOUR;
	
    private static final String STR_JUST_NOW = "just now";
    private static final String STR_MINUTES = " min ";
    private static final String STR_HOURS = " hrs ";
    private static final String STR_DAYS = " days ";
    
	public static final String since(long timestamp) {
		long since = System.currentTimeMillis() - timestamp;
		
		if (since < MINUTE)
			return STR_JUST_NOW;
		
		if (since < HOUR)
			return (since / MINUTE) + STR_MINUTES;
		
		if (since < DAY)
			return (since / HOUR) + STR_HOURS + ((since % HOUR) / MINUTE) + STR_MINUTES;
		
		return (since / DAY) + STR_DAYS + ((since % DAY) / HOUR) + STR_HOURS + ((since % MINUTE) / MINUTE) + STR_MINUTES;
	}

}
