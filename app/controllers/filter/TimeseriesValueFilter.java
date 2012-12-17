package controllers.filter;

import java.util.ArrayList;
import java.util.List;

import uk.bl.monitrix.TimeseriesValue;

public class TimeseriesValueFilter {

	public long x;
	
	public long y;
	
	public TimeseriesValueFilter(TimeseriesValue val) {
		this.x = val.getTimestamp() / 1000;
		this.y = val.getValue();
	}
	
	public static List<TimeseriesValueFilter> map(List<TimeseriesValue> timeseries) {
		List<TimeseriesValueFilter> mapped = new ArrayList<TimeseriesValueFilter>();
		for (TimeseriesValue val : timeseries) {
			mapped.add(new TimeseriesValueFilter(val));
		}
		return mapped;
	}
	
}
