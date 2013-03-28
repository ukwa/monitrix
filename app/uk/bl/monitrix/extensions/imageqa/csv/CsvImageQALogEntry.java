package uk.bl.monitrix.extensions.imageqa.csv;

import uk.bl.monitrix.extensions.imageqa.model.ImageQALogEntry;

public class CsvImageQALogEntry implements ImageQALogEntry {
	
	private String[] fields;
	
	CsvImageQALogEntry(String line) {
		fields = line.split(";");
		if (fields.length != 18)
			throw new RuntimeException("Invalid number of log message fields (expected 19, found " + fields.length + ")");
	}
	
	@Override
	public String getOriginalWebURL() {
		return fields[2];
	}
	
	@Override
	public String getWaybackImageURL() {
		return fields[3];
	}
	
	@Override
	public String getOriginalImageURL() {
		return fields[17];
	}
	
	@Override
	public String getMessage() {
		return fields[8];
	}
	
	@Override
	public String getPSNRMessage()  {
		return fields[16];
	}
	
	public String toString() {
		return getOriginalWebURL() + ", " + getWaybackImageURL() + ", " + getOriginalImageURL() + ", " + getMessage() + ", " + getPSNRMessage();
	}

}
