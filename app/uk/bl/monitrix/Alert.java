package uk.bl.monitrix;

/**
 * An Alert.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class Alert {
	
	private String offendingHost;
	
	private String alertName;
	
	private String alertDescription;
	
	public Alert(String offendingHost, String alertName, String alertDescription) {
		this.offendingHost = offendingHost;
		this.alertName = alertName;
		this.alertDescription = alertDescription;
	}
	
	public String getOffendingHost() {
		return offendingHost;
	}

	public String getAlertName() {
		return alertName;
	}
	
	public String getAlertDescription() {
		return alertDescription;
	}

}
