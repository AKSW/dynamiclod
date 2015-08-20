package dynlod.mongodb.objects;

import java.util.Date;

import com.mongodb.DBObject;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;

public class SystemPropertiesMongoDBObject extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "SystemProperties";

	public static final String DOWNLOADED_LOV = "downloadedLOV";


	// class properties

	private Boolean downloadedLOV;	

	public SystemPropertiesMongoDBObject() {
		super(COLLECTION_NAME, COLLECTION_NAME);
		loadObject();
	}

	public boolean updateObject(boolean checkBeforeInsert) {
		try {
			mongoDBObject.put(DOWNLOADED_LOV, downloadedLOV);
			insert(checkBeforeInsert);
			return true;
		} catch (Exception e2) {
			// e2.printStackTrace();

			try {
				if (update())
					return true;
				else
					return false;
			} catch (DynamicLODGeneralException e) {
				e.printStackTrace();
				return false;
			}
		}
	}

	protected boolean loadObject() {
		DBObject obj = search();

		if (obj != null) {

			downloadedLOV = (Boolean) obj.get(DOWNLOADED_LOV);
			
			return true;
		}
		return false;
	}

	public Boolean getDownloadedLOV() {
		return downloadedLOV;
	}

	public void setDownloadedLOV(Boolean downloadedLOV) {
		this.downloadedLOV = downloadedLOV;
	}

}
