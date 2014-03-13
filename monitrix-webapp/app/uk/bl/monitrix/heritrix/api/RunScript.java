package uk.bl.monitrix.heritrix.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import org.apache.commons.io.IOUtils;


/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class RunScript {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// Set the parameters:
		String uri = "http://static.guim.co.uk/sys-images/Guardian/Pix/pictures/2013/4/11/1365681260808/Egyptian-women-protest-008.jpg";
		uri = "https://www.gov.uk/government/publications?page=2";
		String pathFromSeed = "L";
		
		// Find script, from resource path:
		InputStream in = HeritrixAPI.class.getResourceAsStream("/scripts/deciderule-url-test.groovy");
		// Convert script into a string:
		String script = inputStreamToString(in);
		
		// Insert the parameters into the script:
		String parameters = "uri = \""+uri+"\"\n";
		parameters += "pathFromSeed = \""+pathFromSeed+"\"\n";
		script = parameters + script;
		
		// Connect to H3 instance:
		HeritrixAPI h = new HeritrixAPI(new URL("https://crawler02.bl.uk:8444/"),"admin","bl_uk");
		
		// Issue POST to execute script:
		ScriptResult sr = h.postScript("crawl1-20130412144637", "groovy", script );
		
		// Print results:
		if( sr.getException() != null ) {
			System.out.println("ERROR: \n"+sr.getException());
		}
		if( sr.getRawOutput() != null ) {
			System.out.println("GOT: \n"+sr.getRawOutput());
		}
		
	}

	private static String inputStreamToString( InputStream in ) throws IOException {
		StringWriter writer = new StringWriter();
		IOUtils.copy(in, writer, "UTF-8");
		return writer.toString();
		
	}
}
