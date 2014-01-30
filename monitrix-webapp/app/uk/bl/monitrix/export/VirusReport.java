package uk.bl.monitrix.export;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import play.Play;
import play.Logger;

import uk.bl.monitrix.model.VirusLog;
import uk.bl.monitrix.model.VirusRecord;

import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JasperCompileManager;
import net.sf.jasperreports.engine.JasperExportManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.JasperReport;
import net.sf.jasperreports.engine.data.JRBeanCollectionDataSource;

public class VirusReport {

	private static JasperReport report = null;
	
	private VirusLog virusLog;

	static {
		try {
			InputStream in = Play.class.getClassLoader().getResourceAsStream(
					"virus-report.jrxml");
			report = JasperCompileManager.compileReport(in);
		} catch (JRException e) {
			Logger.error("Could not load virus report PDF template: "
					+ e.getMessage());
			throw new RuntimeException(e);
		}
	}

	public VirusReport(VirusLog virusLog) {
		this.virusLog = virusLog;
	}

	public byte[] toPDF() {
		try {
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			JasperPrint print = JasperFillManager.fillReport(report,
					new HashMap<String, Object>(),
					new JRBeanCollectionDataSource(VirusReport.map(virusLog.getVirusRecords())));
			JasperExportManager.exportReportToPdfStream(print, os);
			os.flush();
			return os.toByteArray();
		} catch (JRException e) {
			// Should never happen
			throw new RuntimeException(e);
		} catch (IOException e) {
			// Should never happen
			throw new RuntimeException(e);
		}
	}
	
	private static List<VirusRecordWrapper> map(Iterator<VirusRecord> virusRecords) {
		List<VirusRecordWrapper> mapped = new ArrayList<VirusRecordWrapper>();
		while (virusRecords.hasNext())
			mapped.add(new VirusRecordWrapper(virusRecords.next()));
		return mapped;
	}

	public static class VirusRecordWrapper {

		private String virusName;

		private Integer occurences;

		public VirusRecordWrapper(VirusRecord record) {
			this.virusName = record.getName();
			Map<String, Integer> occurenceMap = record.getOccurences();
			int total = 0;
			for (Integer count : occurenceMap.values())
				total += count.intValue();
			
			this.occurences = total;
		}

		public String getVirusName() {
			return virusName;
		}

		public Integer getOccurences() {
			return occurences;
		}

		public void setOccurences(Integer occurences) {
			this.occurences = occurences;
		}

	}

}
