package dynlod.API;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class APIMessage {
	
	JSONObject msg = new JSONObject();	

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
	

	
	public String toJSONString() {
		return msg.toString(4);
	}
}
