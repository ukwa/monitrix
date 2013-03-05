package uk.bl.monitrix.heritrix.api;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class AbstractHeritrixResponse {
	
	private static DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	
	protected Document document;
	
	static {
		factory.setNamespaceAware(true);
	}
	
	AbstractHeritrixResponse(InputStream xml) throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilder builder = factory.newDocumentBuilder();
		this.document = builder.parse(xml);
	}

}
