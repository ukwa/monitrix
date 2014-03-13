package uk.bl.monitrix.heritrix.api;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.CoreConnectionPNames;
import org.xml.sax.SAXException;

import play.Logger;

public class HeritrixAPI {
	
	private DefaultHttpClient httpClient;
	
	private URL url;
	
	public HeritrixAPI(URL url, String authUser, String authPassword) {
		this.url = url;
		
		httpClient = new DefaultHttpClient();
		httpClient.getCredentialsProvider().setCredentials(
				new AuthScope(url.getHost(), url.getPort()),
				new UsernamePasswordCredentials(authUser, authPassword)
		);
		
		httpClient.getParams()
				.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 3000)
				.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 3000)
				.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024)
				.setParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, true)
				.setParameter(CoreConnectionPNames.TCP_NODELAY, false);
		
		if (url.getProtocol().toLowerCase().equals("https")) {
			try {
				SSLContext sc = SSLContext.getInstance("SSL");
			    sc.init(null, new TrustManager[] { new UnsafeTrustManager() }, SecureRandom.getInstance("SHA1PRNG"));
			    
			    SSLSocketFactory sf = new SSLSocketFactory(sc, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
			    Scheme httpsScheme = new Scheme("https", url.getPort(), sf);
			    httpClient.getConnectionManager().getSchemeRegistry().register(httpsScheme);	
			} catch (NoSuchAlgorithmException e) {
				// Should never happen
				throw new RuntimeException(e);
			} catch (KeyManagementException e) {
				// Should never happen
				throw new RuntimeException(e);
			}
		}
	}
	
	public String getEndpointURL() {
		return url.toString();
	}
	
	public HeritrixSummary getSummary() throws IllegalStateException, IOException {
		try {
			HttpGet get = new HttpGet(url.toURI());
			get.setHeader("Accept", "application/xml");
			
			HttpResponse response = httpClient.execute(get);		
			return new HeritrixSummary(response.getEntity().getContent());
		} catch (URISyntaxException e) {
			// Should never happen
			throw new RuntimeException(e);
		} catch (ParserConfigurationException e) {
			// Should never happen
			throw new RuntimeException(e);
		} catch (SAXException e) {
			// Should never happen
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Post a script to the Heritrix instance
	 * @param job
	 * @param lang
	 * @param script
	 */
	public ScriptResult postScript(String job, String lang, String script) {
		try {
			// POST it
			URI endpoint = url.toURI().resolve("engine/job/"+job+"/script");
			Logger.info("Using endpoint: "+endpoint);
			HttpPost post = new HttpPost(endpoint);
			post.setHeader("Accept", "application/xml");
			
	        List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(2);
	        nameValuePairs.add(new BasicNameValuePair("engine", lang));
	        nameValuePairs.add(new BasicNameValuePair("script", script));
	        post.setEntity(new UrlEncodedFormEntity(nameValuePairs));

	        // Execute HTTP Post Request
			HttpResponse response = httpClient.execute(post);			        
			return new ScriptResult(response.getEntity().getContent());
	    } catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
