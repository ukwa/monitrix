package uk.bl.monitrix.ingest;

import java.io.File;

import org.junit.Test;

import akka.actor.ActorSystem;

public class IngestorPoolTest {
	
	// private static final String LOG_FILE = "test/sample-log-1E3.txt";
	private static final String LOG_FILE = "/home/simonr/Downloads/sample-log-2E6.log";
	
	private ActorSystem system = ActorSystem.create("Test");
	
	private IngestorPool pool = new IngestorPool(new DummyBatchImporter(), system);
		
	@Test
	public void testPoolSetup() throws InterruptedException {
		File f = new File(LOG_FILE);
		
		pool.addHeritrixLog(f.getAbsolutePath());
		while (pool.isAlive(f.getAbsolutePath())) {
			IngestorStatus status = pool.getStatus(f.getAbsolutePath());
			System.out.println("Status: " + status.phase + " (" + status.progress + "%)");
			if (status.phase.equals(IngestorStatus.Phase.TRACKING))
				break;
			Thread.sleep(5000);
		}
		system.shutdown();
		system.awaitTermination();
	}

}
