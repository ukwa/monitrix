package uk.bl.monitrix.export;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;

import net.sf.jasperreports.engine.JREmptyDataSource;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JasperCompileManager;
import net.sf.jasperreports.engine.JasperExportManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.JasperReport;

public class JasperTest {
	
	public static void main(String[] args) throws FileNotFoundException {
		JasperReport jasperReport;
	    JasperPrint jasperPrint;
	    try {
	      jasperReport = JasperCompileManager.compileReport(new FileInputStream("app/uk/bl/monitrix/export/jasper-helloworld.jrxml"));
	      jasperPrint = JasperFillManager.fillReport(jasperReport, new HashMap<String, Object>(), new JREmptyDataSource());
	      JasperExportManager.exportReportToPdfFile(jasperPrint, "simple_report.pdf");
	    } catch (JRException e) {
	      e.printStackTrace();
	    }
	}

}
