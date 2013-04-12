/**
 * 
 */
package uk.bl.monitrix.heritrix.api;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ScriptResult extends AbstractHeritrixResponse {

	private NodeList topLevel;
	
	ScriptResult(InputStream xml) throws ParserConfigurationException,
			SAXException, IOException {
		super(xml);
		topLevel = document.getDocumentElement().getChildNodes();
	}
	
	public String getRawOutput() {
		return getTextContentOfNode("rawOutput", topLevel);
	}

	public String getException() {
		return getTextContentOfNode("exception", topLevel);
	}

}
