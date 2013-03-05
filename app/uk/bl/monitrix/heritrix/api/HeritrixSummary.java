package uk.bl.monitrix.heritrix.api;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

public class HeritrixSummary extends AbstractHeritrixResponse {

	HeritrixSummary(InputStream xml) throws ParserConfigurationException, SAXException, IOException {
		super(xml);
	}

}
