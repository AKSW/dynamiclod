package dataid.API;

public class APIMessage {

	
	String message;
	
	Boolean error;
	
	public APIMessage(boolean error, String message) {
		this.message = message;
		this.error = error;
	}
	
	@Override
	public String toString() {
		return "Success: "+error+". Message: "+ message;
	}
	
}
