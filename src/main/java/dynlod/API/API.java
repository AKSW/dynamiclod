package dynlod.API;

import java.util.ArrayList;

import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;

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
		JsonObject j = new JsonObject();
		JsonArray ja = new JsonArray();
		
		for (APIMessage msg : message) {
			ja.add(msg.toJSON());
		}
		j.put("messages",ja);
		return j.toString();
	}

	public void addMessage(APIMessage message) {
		this.message.add(message);
	}

}
