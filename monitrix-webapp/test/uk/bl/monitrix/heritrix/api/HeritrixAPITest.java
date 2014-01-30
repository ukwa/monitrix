package uk.bl.monitrix.heritrix.api;

import java.io.IOException;
import java.net.URL;

import org.junit.Test;

public class HeritrixAPITest {

	@Test
	public void testHeritrixAPI() throws IllegalStateException, IOException {
		HeritrixAPI api = new HeritrixAPI(new URL("https://172.20.30.201:8083"), "admin", "admin");
		HeritrixSummary summary = api.getSummary();
		System.out.println(summary.toString());
	}
	
	/*		
	DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	factory.setNamespaceAware(true); // never forget this!
	DocumentBuilder builder = factory.newDocumentBuilder();
	Document doc = builder.parse(entity.getContent());
	
	XPathFactory xpf = XPathFactory.newInstance();
	XPath xpath = xpf.newXPath();
	XPathExpression expr = xpath.compile("//jobs/value");
	
	NodeList result = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
	for (int i=0; i<result.getLength(); i++) {
		System.out.println(result.item(i));
	}
	*/
	
}
