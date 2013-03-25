package uk.bl.monitrix.extensions.imageqa;

public class QATextLogEntry {
	
	private String[] fields;
	
	QATextLogEntry(String line) {
		fields = line.split(";");
		if (fields.length != 18)
			throw new RuntimeException("Invalid number of log message fields (expected 19, found " + fields.length + ")");
	}
	
	public String getOriginalWebURL() {
		return fields[2];
	}
	
	public String getWaybackImageURL() {
		return fields[3];
	}
	
	public String getOriginalImageURL() {
		return fields[17];
	}
	
	public String getMessage() {
		return fields[8];
	}
	
	public String getPSNRMessage()  {
		return fields[16];
	}
	
	public String toString() {
		return getOriginalWebURL() + ", " + getWaybackImageURL() + ", " + getOriginalImageURL() + ", " + getMessage() + ", " + getPSNRMessage();
	}

}
