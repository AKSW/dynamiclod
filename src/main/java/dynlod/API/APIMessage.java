package dynlod.API;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONObject;

public class APIMessage {

	Boolean error;
	
	String key; 

	String message;

	HashMap<String, String> extraMessages = new HashMap<String, String>();
	

	public APIMessage(String key, boolean error, String message) {
		this.message = message;
		this.error = error;
		this.key = key;
	}

	public APIMessage(String key, boolean error, String message,
			HashMap<String, String> extraMessages) {
		this.message = message;
		this.error = error;
		this.extraMessages = extraMessages;
		this.key = key;
	}

	@Override
	public String toString() {
		return "Success: " + error + ". Message: " + message;
	}

	public JSONObject toJSON() {
		JSONObject j = new JSONObject();
		j.put("success", error);
		j.put("message", message);

		Iterator it = extraMessages.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			j.put((String) pair.getKey(), (String) pair.getValue());
		}

		JSONObject returnObject = new JSONObject(); 
		returnObject.put(key, j); 
		
		return returnObject;
	}
}
