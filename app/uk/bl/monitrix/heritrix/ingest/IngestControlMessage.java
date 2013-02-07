package uk.bl.monitrix.heritrix.ingest;

/**
 * A message class for use in communcation with the {@link IngestActor}.
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 */
public class IngestControlMessage {
	
	public enum Command { START, STOP, GET_STATUS, CHANGE_SLEEP_INTERVAL, ADD_WATCHED_LOG }
	
	private Command cmd;
	
	private Object payload;
	
	public IngestControlMessage(Command cmd) {
		this.cmd = cmd;
	}
	
	public IngestControlMessage(Command cmd, Object payload) {
		this.cmd = cmd;
		this.payload = payload;
	}
	
	public Command getCommand() {
		return cmd;
	}
	
	public Object getPayload() {
		return payload;
	}

}
