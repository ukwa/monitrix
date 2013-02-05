package uk.bl.monitrix.ingest;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import play.Logger;
import uk.bl.monitrix.database.DBBatchImporter;

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

/**
 * A management component that oversees a 'pool' of Heritrix log files, each being
 * tracked by one {@link IngestActor}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class IngestorPool {
	
	private DBBatchImporter importer;
	
	private ActorSystem system;
	
	private Map<String, ActorRef> ingestActors = new HashMap<String, ActorRef>();
	
	public IngestorPool(DBBatchImporter importer, ActorSystem system) {
		this.importer = importer;
		this.system = system;
	}
	 
	/**
	 * Adds a new log file to the pool.
	 * @param heritrixLog the log file
	 * @return <code>false</code> if the file could not be opened
	 */
	public boolean addHeritrixLog(String heritrixLog) {
		final File f = new File(heritrixLog);
		if (!f.exists())
			return false;
		
		ActorRef ingestActor = system.actorOf(new Props(new UntypedActorFactory() {	
			private static final long serialVersionUID = 1373336745955431875L;

			@Override
			public Actor create() {
				try {
					return new IngestActor(f, importer, system);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}), "ingest-actor-" + ingestActors.size());
		
		ingestActors.put(heritrixLog, ingestActor);
		ingestActor.tell(IngestActor.Messages.START);
		return true;
	}
	
	/**
	 * Gets the list of currently tracked Heritrix logs (absolute paths).
	 * @return the logs
	 */
	public List<String> getTrackedLogs() {
		return new ArrayList<String>(ingestActors.keySet());
	}
	
	/**
	 * Checks wether the tracking actor for a specific log is still alive.
	 * @param log the log path
	 * @return <code>true</code> if the tracking actor is still alive
	 */
	public boolean isAlive(String log) {
		ActorRef actor = ingestActors.get(log);
		if (actor == null) {
			Logger.info("isAlive called for non-existing ingest actor: " + log);
			return false;
		} else {
			return !actor.isTerminated();
		}
	}
	
	/**
	 * Returns the current status of the tracking actor for a specific log.
	 * @param log the log path
	 * @return the status of the tracking actor for this log
	 */
	public IngestorStatus getStatus(String log) {
		ActorRef actor = ingestActors.get(log);
		if (actor == null)
			return new IngestorStatus(IngestorStatus.Phase.UNKNOWN);
		
		Future<Object> future = 
				Patterns.ask(actor, IngestActor.Messages.GET_STATUS, new Timeout(Duration.create(5, TimeUnit.SECONDS)));
		
		try {
			Object result = Await.result(future, Duration.create(30, TimeUnit.SECONDS));
			return (IngestorStatus) result;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
