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
		ingestActor.tell(new IngestControlMessage(Command.SYNC_WITH_SCHEDULE));
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
