package uk.bl.monitrix.heritrix.ingest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import play.Logger;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActorFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import akka.pattern.Patterns;
import scala.concurrent.duration.Duration;
import akka.util.Timeout;

import uk.bl.monitrix.database.DBIngestConnector;
import uk.bl.monitrix.heritrix.ingest.IngestControlMessage.Command;

/**
 * The IngestWatcher provides a control entry point into the ingest system. The actual work 
 * of monitoring and ingest is handled by the {@link IngestActor}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class IngestWatcher {
	
	private ActorRef ingestActor;
	
	public IngestWatcher(final DBIngestConnector db, final ActorSystem system) {
		this.ingestActor = system.actorOf(new Props(new UntypedActorFactory() {	
			private static final long serialVersionUID = 1373336745955431875L;

			@Override
			public Actor create() {
				return new IngestActor(db, system);
			}
		}), "ingest-actor");
				
		refresh();
	}

	/**
	 * Re-syncs the status of the IngestWatcher with the DB-backed IngestSchedule
	 */
	public void refresh() {
		ingestActor.tell(new IngestControlMessage(Command.SYNC_WITH_SCHEDULE), null); // FIXME NULL?!?!
	}
	
	/**
	 * Starts the watcher.
	 */
	public void startWatching() {
		Logger.info("Starting ingest watcher");
		ingestActor.tell(new IngestControlMessage(Command.START), null); // FIXME NULL?!?!
	}
	
	/**
	 * Stops the watcher.
	 */
	public void stopWatching() {
		Logger.info("Stopping ingest watcher");
		ingestActor.tell(new IngestControlMessage(Command.STOP), null); // FIXME NULL?!?!
	}
	
	/**
	 * Returns true if the underlying ingest actor is still alive, or whether
	 * it has crashed.
	 * @return <code>true</code> if the actor is alive and kicking
	 */
	public boolean isAlive() {
		return true;
	}
	
	/**
	 * Returns true if the ingest process is running - i.e. if the actor is alive
	 * and has not yet stopped the ingest loop.
	 * @return <code>true</code> if the ingest loop is running
	 */
	public boolean isRunning() {
		if (!isAlive())
			return false;
		
		try {
			Future<Object> future = 
					Patterns.ask(ingestActor,  new IngestControlMessage(Command.CHECK_RUNNING),  new Timeout(Duration.create(5, TimeUnit.SECONDS)));
			Object result = Await.result(future, Duration.create(30, TimeUnit.SECONDS));
			return (Boolean) result;
		} catch (Exception e) {
			return false;
		}
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
