package uk.bl.monitrix;

/**
 * A value in a pie chart (name, value).
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class PieChartValue {
	
	private String name;
	
	private long value;
	
	public PieChartValue(String name, long value) {
		this.name = name;
		this.value = value;
	}
	
	public String getName() {
		return name;
	}
	
	public long getValue() {
		return value;
	}

}
