package dataid.API;

public abstract class API extends Thread{
	
	APIMessage message;
	
	public abstract void run();
	
	public API() {
		setMessage(new APIMessage(true, "API initialized."));
		
	}

	public APIMessage getMessage() {
		return message;
	}
	

	public void setMessage(APIMessage message) {
		this.message = message;
	}
	
	

}
