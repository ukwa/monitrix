package uk.bl.monitrix;

/**
 * Helper functions for number and date formatting.
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
    private static final String HOURS_COMPACT = "h ";
    
    private static final String DAYS_NORMAL = " days ";
    private static final String DAYS_COMPACT = "d ";
    
    public static String since(long timestamp) {
    	return since(timestamp, false);
    }
    
	public static String since(long timestamp, boolean compact) {
		return formatDuration(System.currentTimeMillis() - timestamp, compact);
	}
	
	public static String formatDuration(long durationMillis) {
		return formatDuration(durationMillis, false);
	}
		
	public static String formatDuration(long durationMillis, boolean compact) {
		if (durationMillis < MINUTE)
			return JUST_NOW;
		
		if (durationMillis < HOUR)
			return (durationMillis / MINUTE) + MINUTES_NORMAL;
		
		if (durationMillis < DAY) {
			if (compact)
				return (durationMillis / HOUR) + HOURS_COMPACT + ((durationMillis % HOUR) / MINUTE) + MINUTES_COMPACT;
			else
				return (durationMillis / HOUR) + HOURS_NORMAL + ((durationMillis % HOUR) / MINUTE) + MINUTES_NORMAL;
		}
		
		if (compact)
			return (durationMillis / DAY) + DAYS_COMPACT + ((durationMillis % DAY) / HOUR) + HOURS_COMPACT;
		else
			return (durationMillis / DAY) + DAYS_NORMAL + ((durationMillis % DAY) / HOUR) + HOURS_NORMAL + ((durationMillis % MINUTE) / MINUTE) + MINUTES_NORMAL;
	}

}
