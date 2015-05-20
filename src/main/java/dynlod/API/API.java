package dynlod.API;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;


public abstract class API extends Thread{
	
	ArrayList<APIMessage> message = new ArrayList<APIMessage>();
	
	public abstract void run();
	
	public API() {
		addMessage(new APIMessage("coreMsg",true, "API initialized."));
		
	}

	public String getMessage() {
		StringBuilder r = new StringBuilder();
		for (APIMessage msg : message) {
			r.append(msg.toString()+"\n");
		}
		return r.toString();
	}
	
	public String getMessageJSON() { 
		JSONObject j = new JSONObject(); 
		JSONArray ja = new JSONArray();
		
		for (APIMessage msg : message) {
			ja.put(msg.toJSON());
		}
		j.put("messages",ja);
		return j.toString();
	}

	public void addMessage(APIMessage message) {
		this.message.add(message);
	}

}
