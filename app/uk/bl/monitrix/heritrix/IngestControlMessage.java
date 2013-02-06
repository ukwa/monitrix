package uk.bl.monitrix.heritrix;

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
