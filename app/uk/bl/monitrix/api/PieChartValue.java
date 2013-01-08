package uk.bl.monitrix.api;

/**
 * A value in a pie chart (name, value).
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class PieChartValue {
	
	private String label;
	
	private long value;
	
	public PieChartValue(String label, long value) {
		this.label = label;
		this.value = value;
	}
	
	public String getLabel() {
		return label;
	}
	
	public long getValue() {
		return value;
	}

}
