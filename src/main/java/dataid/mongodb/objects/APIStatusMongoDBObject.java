package dataid.mongodb.objects;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

import dataid.exceptions.DataIDException;
import dataid.mongodb.DataIDDB;

public class APIStatusMongoDBObject extends DataIDDB {
	 
	public APIStatusMongoDBObject(String uri) {
		super(COLLECTION_NAME, uri);
		loadObject();
	}

	// Collection name
	public static final String COLLECTION_NAME = "APIStatus";
	
	public static final String MESSAGE = "message";

	public static final String HAS_ERROR = "hasError";
	
	

	// class properties
	private String message;
	
	private boolean hasError = false;
	


	public boolean updateObject(boolean checkBeforeInsert) {

		// save object case it doens't exists
		try {
			mongoDBObject.put(MESSAGE, message);
			
			mongoDBObject.put(HAS_ERROR, hasError);
			
			insert(checkBeforeInsert);
			return true;
		} catch (Exception e2) {
//			e2.printStackTrace();

			try {
				if (update())
					return true;
				else
					return false;
			} catch (DataIDException e) {
				e.printStackTrace();
				return false;
			}
		}
	}

	protected boolean loadObject() {
		DBObject obj = search();

		if (obj != null) {
			// mongoDBObject = (BasicDBObject) obj;

			message = (String) obj.get(MESSAGE);
			hasError = (Boolean) obj.get(HAS_ERROR);

			return true;
		}
		return false;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
		updateObject(true);
	}

	public boolean getHasError() {
		return hasError;
	}

	public void setHasError(boolean error) {
		this.hasError = error;
		updateObject(true);
	}	
	
	
}
