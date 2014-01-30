package uk.bl.monitrix.heritrix.api;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class AbstractHeritrixResponse {
	
	private static DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
	
	private static XPathFactory xpathFactory = XPathFactory.newInstance();
	
	protected Document document;
	
	static {
		docBuilderFactory.setNamespaceAware(true);
	}
	
	AbstractHeritrixResponse(InputStream xml) throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
		this.document = builder.parse(xml);
	}
	
	public static String getTextContentOfNode(String nodeName, NodeList nodes) {
		for (int i=0; i<nodes.getLength(); i++) {
			Node n = nodes.item(i);
			if (n.getNodeName().equalsIgnoreCase(nodeName)) {
				return n.getTextContent();
			}
		}
		
		return null;
	}
	
	public static NodeList getChildrenOfNode(String nodeName, NodeList nodes) {
		for (int i=0; i<nodes.getLength(); i++) {
			Node n = nodes.item(i);
			if (n.getNodeName().equalsIgnoreCase(nodeName)) {
				return n.getChildNodes();
			}
		}
		
		return null;		
	}
	
	public static NodeList xPath(Node rootNode, String xpath) {
		try {
			XPath x = xpathFactory.newXPath();
			XPathExpression expr = x.compile(xpath);
			return (NodeList) expr.evaluate(rootNode, XPathConstants.NODESET);
		} catch (XPathExpressionException e) {
			// Should never happen!
		}
		return null;
	}
	
}
