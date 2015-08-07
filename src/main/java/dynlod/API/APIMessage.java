package dynlod.API;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class APIMessage {
	
	JSONObject msg = new JSONObject();	
	
	public void setCoreMsgSuccess(){
		msg.put("coreMsg", "API successfully initialized.");
	}

	public void setCoreMsgError(String error){
		msg.put("coreMsg", "API not initialized. "+ error);
	}
	
	public void setParserMsg(String message){
		msg.put("parserMsg", message);
	}
	
	public void addStatisticsMsg(JSONObject message){
		msg.put("statistics", message); 
	}
	
	public void addDistributionMsg(JSONObject object){
		try{
			JSONArray a = (JSONArray) msg.get("distributions");
			a.put(object);	
		}
		catch (Exception j){ 
			msg.put("distributions", new JSONArray().put(object));
		}
	}
	
	
	public APIMessage() {
		// TODO Auto-generated constructor stub
	}
	

	
	public String toJSONString() {
		return msg.toString(4);
	}
}
