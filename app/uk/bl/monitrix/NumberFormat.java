package uk.bl.monitrix;

/**
 * Various helper functions for number and date formatting.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class NumberFormat {
	
    private static final int MINUTE = 60000;
    private static final int HOUR = 60 * MINUTE;
    private static final int DAY = 24 * HOUR;
	
    private static final String JUST_NOW = "just now";
    
    private static final String MINUTES_NORMAL = " min ";
    private static final String MINUTES_COMPACT = "m";
    
    private static final String HOURS_NORMAL = " hrs ";
    private static final String HOURS_COMPACT = "h";
    
    private static final String DAYS_NORMAL = " days ";
    private static final String DAYS_COMPACT = "d";
    
    public static final String since(long timestamp) {
    	return since(timestamp, false);
    }
    
	public static final String since(long timestamp, boolean compact) {
		long since = System.currentTimeMillis() - timestamp;
		
		if (since < MINUTE)
			return JUST_NOW;
		
		if (since < HOUR)
			return (since / MINUTE) + MINUTES_NORMAL;
		
		if (since < DAY) {
			if (compact)
				return (since / HOUR) + HOURS_COMPACT + ((since % HOUR) / MINUTE) + MINUTES_COMPACT;
			else
				return (since / HOUR) + HOURS_NORMAL + ((since % HOUR) / MINUTE) + MINUTES_NORMAL;
		}
		
		if (compact)
			return (since / DAY) + DAYS_COMPACT + ((since % DAY) / HOUR) + HOURS_COMPACT;
		else
			return (since / DAY) + DAYS_NORMAL + ((since % DAY) / HOUR) + HOURS_NORMAL + ((since % MINUTE) / MINUTE) + MINUTES_NORMAL;
	}

}
