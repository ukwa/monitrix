package controllers.filter;

import java.util.AbstractList;
import java.util.List;

import uk.bl.monitrix.TimeseriesValue;

/**
 * A simple class that wraps a {@link TimeseriesValue} so that it can be directly 
 * serialized to JSON by Play. 
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class TimeseriesValueFilter {

	public long x;
	
	public long y;
	
	public TimeseriesValueFilter(TimeseriesValue val) {
		this.x = val.getTimestamp() / 1000;
		this.y = val.getValue();
	}
	
	/**
	 * Utility method to lazily map a list of {@link TimeseriesValue} objects
	 * to a list of JSON-compatible wrappers.
	 * @param timeseries the timeseries
	 * @return the wrapped list
	 */
	public static List<TimeseriesValueFilter> map(final List<TimeseriesValue> timeseries) {
		return new AbstractList<TimeseriesValueFilter>() {
			
			@Override
			public TimeseriesValueFilter get(int index) {
				return new TimeseriesValueFilter(timeseries.get(index));
			}

			@Override
			public int size() {
				return timeseries.size();
			}
			
		};
	}
	
}
