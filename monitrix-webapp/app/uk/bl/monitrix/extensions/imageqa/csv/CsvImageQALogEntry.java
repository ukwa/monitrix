package uk.bl.monitrix.extensions.imageqa.csv;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import uk.bl.monitrix.extensions.imageqa.model.ImageQALogEntry;

/**
 * Example log line (with line breaks inserted at ';' where appropriate):
 * 
 * 2013-03-21 16:38:01.850688;1.07782793045;http://www.acses.org.uk/;
 * http://www.webarchive.org.uk/thumbs/66158797/66128255c.jpg;66158797;
 * 1681;52;252;VERY DIFFERENT;60;30;0;438699;21483;2.6929;3.0;
 * DIFFERENT;http://127.0.0.1:8000/qa/orig/1363883878.13.png;
 * 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class CsvImageQALogEntry implements ImageQALogEntry {
	
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private String[] fields;
	
	CsvImageQALogEntry(String line) {
		fields = line.split(";");
		if (fields.length != 18)
			throw new RuntimeException("Invalid number of log message fields (expected 19, found " + fields.length + ")");
	}

	@Override
	public Date getTimestamp() {
		try {
			return DATE_FORMAT.parse(fields[0]);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public double getExecutionTime() {
		return Double.parseDouble(fields[1]);
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
	public long getWaybackTimestamp() {
		return Long.parseLong(fields[4]);
	}
		
	@Override
	public int getFC1() {
		return Integer.parseInt(fields[5]);
	}

	@Override
	public int getFC2() {
		return Integer.parseInt(fields[6]);
	}

	@Override
	public int getMC() {
		return Integer.parseInt(fields[7]);
	}

	@Override
	public String getMessage() {
		return fields[8];
	}
		
	@Override
	public int getTS1() {
		return Integer.parseInt(fields[9]);
	}

	@Override
	public int getTS2() {
		return Integer.parseInt(fields[10]);
	}

	@Override
	public int getOCR() {
		return Integer.parseInt(fields[11]);
	}

	@Override
	public int getImage1Size() {
		return Integer.parseInt(fields[12]);
	}

	@Override
	public int getImage2Size() {
		return Integer.parseInt(fields[13]);
	}

	@Override
	public double getPSNRSimilarity() {
		return Double.parseDouble(fields[14]);
	}

	@Override
	public double getPSNRThreshold() {
		return Double.parseDouble(fields[15]);
	}
	
	@Override
	public String getPSNRMessage()  {
		return fields[16];
	}
			
	@Override
	public String getOriginalImageURL() {
		return fields[17];
	}
	
	@Override
	public String toString() {
		return getOriginalWebURL() + ", " + getWaybackImageURL() + ", " + getOriginalImageURL() + ", " + getMessage() + ", " + getPSNRMessage();
	}

}
