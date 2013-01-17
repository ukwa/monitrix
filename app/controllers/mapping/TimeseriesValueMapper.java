package controllers.mapping;

import java.util.AbstractList;
import java.util.List;

import uk.bl.monitrix.analytics.TimeseriesValue;

/**
 * Wraps a {@link TimeseriesValue} so that it can be directly serialized to JSON by Play. 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class TimeseriesValueMapper {

	public long x;
	
	public long y;
	
	public TimeseriesValueMapper(TimeseriesValue val) {
		this.x = val.getTimestamp() / 1000;
		this.y = val.getValue();
	}
	
	/**
	 * Utility method to lazily map a list of {@link TimeseriesValue} objects
	 * to a list of JSON-compatible wrappers.
	 * @param timeseries the timeseries
	 * @return the wrapped list
	 */
	public static List<TimeseriesValueMapper> map(final List<TimeseriesValue> timeseries) {
		return new AbstractList<TimeseriesValueMapper>() {
			
			@Override
			public TimeseriesValueMapper get(int index) {
				return new TimeseriesValueMapper(timeseries.get(index));
			}

			@Override
			public int size() {
				return timeseries.size();
			}
			
		};
	}
	
}
