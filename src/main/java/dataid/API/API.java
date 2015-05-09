package dataid.API;

import java.util.ArrayList;

public abstract class API extends Thread{
	
	ArrayList<APIMessage> message = new ArrayList<APIMessage>();
	
	public abstract void run();
	
	public API() {
		addMessage(new APIMessage(true, "API initialized."));
		
	}

	public String getMessage() {
		StringBuilder r = new StringBuilder();
		for (APIMessage msg : message) {
			r.append(msg.toString()+"\n");
		}
		return r.toString();
	}
	

	public void addMessage(APIMessage message) {
		this.message.add(message);
	}

}
