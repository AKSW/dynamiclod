package dynlod.API;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class APIMessage {
	
	JSONObject msg = new JSONObject();
	
	JSONObject coreMsg = new JSONObject();
	
	JSONObject parserMsg = new JSONObject(); 
	
	JSONObject distributionsMsg = new JSONObject().put("distributions", new JSONArray()); 
	

	Boolean error;
	
	String key; 

	String message;

	HashMap<String, String> extraMessages = new HashMap<String, String>();
	
	public void setCoreMsgSuccess(){
		msg.put("coreMsg", "API successfully initialized.");
	}

	public void setCoreMsgError(String error){
		msg.put("coreMsg", "API not initialized. "+ error);
	}
	
	public void setParserMsg(String message){
		msg.put("parserMsg", message);
	}
	
	public void addDistributionMsg(JSONObject object){
		try{
		
			JSONArray a = (JSONArray) msg.get("distributions");
			a.put(object);	
		}
		catch (JSONException j){ 
			msg.put("distributions", object);
		}
	}
	
	
	public APIMessage() {
		// TODO Auto-generated constructor stub
	}
	


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

	public JSONObject toJSON2() {
		JSONObject j = new JSONObject();
		j.put("success", error);
		j.put("message", message);
//
//		Iterator it = extraMessages.entrySet().iterator();
//		while (it.hasNext()) {
//			Map.Entry pair = (Map.Entry) it.next();
//			j.put((String) pair.getKey(), (String) pair.getValue());
//		}

		JSONObject returnObject = new JSONObject(); 
		returnObject.put(key, j); 
		
		return returnObject;
	}
	
	public String toJSONString() {
		return msg.toString(4);
	}
}
