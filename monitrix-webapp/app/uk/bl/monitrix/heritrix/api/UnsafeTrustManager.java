package uk.bl.monitrix.heritrix.api;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.X509TrustManager;

/**
 * A trust manager that trusts all certificates. Needed for HTTPS-connecting to a Heritrix
 * instance with a self-signed certificate. 
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class UnsafeTrustManager implements X509TrustManager {

	@Override
	public void checkClientTrusted(X509Certificate[] arg0, String arg1) 
			throws CertificateException {  }

	@Override
	public void checkServerTrusted(X509Certificate[] arg0, String arg1)
			throws CertificateException {	}

	@Override
	public X509Certificate[] getAcceptedIssuers() {
		return null;
	}

}
