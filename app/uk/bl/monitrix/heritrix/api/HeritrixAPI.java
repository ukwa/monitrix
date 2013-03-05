package uk.bl.monitrix.heritrix.api;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.xml.sax.SAXException;

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

}
