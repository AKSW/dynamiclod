package dynlod.API.core;

import java.util.ArrayList;


public abstract class API extends Thread{
	
	ArrayList<APIMessage> message = new ArrayList<APIMessage>();
	
	public abstract void run();
	
	public APIMessage apiMessage = new APIMessage();
	
	public API() {
		
	}

}
