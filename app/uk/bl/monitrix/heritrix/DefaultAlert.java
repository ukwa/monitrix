package uk.bl.monitrix.heritrix;

import uk.bl.monitrix.model.Alert;

/**
 * An in-memory implementation of {@link Alert}, for use with {@link LogfileReader}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
class DefaultAlert implements Alert {
	
	private String offendingHost;
	
	private AlertType type;
	
	private String description;
	
	public DefaultAlert(String offendingHost, AlertType type, String description) {
		this.offendingHost = offendingHost;
		this.type = type;
		this.description = description;
	}

	@Override
	public String getOffendingHost() {
		return offendingHost;
	}

	@Override
	public AlertType getAlertType() {
		return type;
	}

	@Override
	public String getAlertDescription() {
		return description;
	}

}
