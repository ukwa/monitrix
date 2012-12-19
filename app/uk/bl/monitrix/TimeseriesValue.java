package uk.bl.monitrix;

/**
 * A coordinate pair in a time-series graph (timestamp -> value).
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class TimeseriesValue implements Comparable<TimeseriesValue> {
	
	private long timestamp;
	
	private long value;
	
	public TimeseriesValue(long timestamp, long value) {
		this.timestamp = timestamp;
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}
	
	public long getValue() {
		return value;
	}
	
	@Override
	public int compareTo(TimeseriesValue other) {
		return (int) (this.timestamp - other.timestamp);
	}
	
}
