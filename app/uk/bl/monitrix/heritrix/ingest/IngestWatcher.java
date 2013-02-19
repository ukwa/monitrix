package uk.bl.monitrix.heritrix.ingest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import play.Configuration;
import play.Logger;
import play.Play;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActorFactory;
import akka.dispatch.Await;
import akka.dispatch.Future;
import akka.pattern.Patterns;
import akka.util.Duration;
import akka.util.Timeout;

import uk.bl.monitrix.database.DBIngestConnector;
import uk.bl.monitrix.heritrix.ingest.IngestControlMessage.Command;

/**
 * The IngestWatcher provides a control entry point into the ingest system. The actual work 
 * of monitoring and ingest is handled by the {@link IngestActor}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class IngestWatcher {
	
	private static Configuration config = Play.application().configuration();
	
	private ActorRef ingestActor;
	
	private List<String> watchedLogs = new ArrayList<String>();
	
	public IngestWatcher(final DBIngestConnector db, final ActorSystem system) {
		this.ingestActor = system.actorOf(new Props(new UntypedActorFactory() {	
			private static final long serialVersionUID = 1373336745955431875L;

			@Override
			public Actor create() {
				try {
					boolean doIncrementalSync = doIncrementalSync();
					Logger.info("Incremental sync set to " + Boolean.toString(doIncrementalSync).toUpperCase());
					return new IngestActor(db, system, doIncrementalSync);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}), "ingest-actor");
				
		for (String path : db.getIngestedLogs())
			addLog(path);
		
		startWatching();
	}
	
	private boolean doIncrementalSync() {
		String incrementalSync = config.getString("monitrix.incremental.sync");
		if (incrementalSync == null)
			return true;
		
		if (incrementalSync.trim().toLowerCase().equals("false"))
			return false;
		
		return true;
	}
	
	/**
	 * Returns the names of the logs currently being watched.
	 * @return the list of log names
	 */
	public List<String> getWatchedLogs() {
		return watchedLogs;
	}
	
	/**
	 * Adds a new log to the list of watched logs.
	 * @param path the log path
	 */
	public void addLog(String path) {
		watchedLogs.add(path);
		ingestActor.tell(new IngestControlMessage(Command.ADD_WATCHED_LOG, path));
	}
	
	/**
	 * Starts the watcher.
	 */
	public void startWatching() {
		Logger.info("Starting ingest watcher");
		ingestActor.tell(new IngestControlMessage(Command.START));
	}
	
	/**
	 * Stops the watcher.
	 */
	public void stopWatching() {
		Logger.info("Stopping ingest watcher");
		ingestActor.tell(new IngestControlMessage(Command.STOP));
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, IngestStatus> getStatus() {
		try {
			Future<Object> future = 
					Patterns.ask(ingestActor, new IngestControlMessage(Command.GET_STATUS), new Timeout(Duration.create(5, TimeUnit.SECONDS)));

			Object result = Await.result(future, Duration.create(30, TimeUnit.SECONDS));
			return (Map<String, IngestStatus>) result;
		} catch (Exception e) {
			return new HashMap<String, IngestStatus>();
		}
	}

}
