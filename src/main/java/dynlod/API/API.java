package dynlod.API;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;


public abstract class API extends Thread{
	
	ArrayList<APIMessage> message = new ArrayList<APIMessage>();
	
	public abstract void run();
	
	public APIMessage apiMessage = new APIMessage();
	
	public API() {
		
	}

}
