package uk.bl.monitrix.email;

import com.typesafe.plugin.MailerAPI;
import com.typesafe.plugin.MailerPlugin;

/**
 * Just an example for now!
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class Mailer {
	
	public static void sendMail(String subject, String from, String text, String... recipients) {
		MailerAPI mail = play.Play.application().plugin(MailerPlugin.class).email();
		mail.setSubject(subject);
		mail.addRecipient(recipients);
		mail.addFrom(from);
		mail.send(text);
	}

}
