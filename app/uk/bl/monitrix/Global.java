package uk.bl.monitrix;

import play.Application;
import play.GlobalSettings;

public class Global extends GlobalSettings {
	
	@Override
	public void onStart(Application app) {
		// TODO read log file
	}  
	
	@Override
	public void onStop(Application app) {
		// Cleanup
	}  
	
}
