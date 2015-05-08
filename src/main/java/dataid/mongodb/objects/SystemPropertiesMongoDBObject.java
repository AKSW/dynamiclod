package dataid.mongodb.objects;

import java.util.Date;

import com.mongodb.DBObject;

import dataid.exceptions.DataIDException;
import dataid.mongodb.DataIDDB;

public class SystemPropertiesMongoDBObject extends DataIDDB {

	// Collection name
	public static final String COLLECTION_NAME = "systemProperties";

	public static final String DOWNLOADED_LOV = "downloadedLOV";

	public static final String LINKSET_TIME_STARTED = "linksetTimeStarted";

	public static final String LINKSET_TIME_FINISHED = "linksetTimeFinished";

	public static final String LINKSET_NEXT_ROUND = "linksetNextRound";

	public static final String LINKSET_STATUS = "linksetStatus";

	// class properties

	private Boolean downloadedLOV;
	
	private Date linksetTimeStarted; 
	
	private Date linksetTimeFinished; 
	
	private Date linksetNextRound; 
	
	private String linksetStatus;
	

	public SystemPropertiesMongoDBObject() {
		super(COLLECTION_NAME, COLLECTION_NAME);
		loadObject();
	}

	public boolean updateObject(boolean checkBeforeInsert) {
		try {
			mongoDBObject.put(DOWNLOADED_LOV, downloadedLOV);

			mongoDBObject.put(LINKSET_TIME_STARTED, linksetTimeStarted);

			mongoDBObject.put(LINKSET_TIME_FINISHED, linksetTimeFinished);

			mongoDBObject.put(LINKSET_NEXT_ROUND, linksetNextRound);

			mongoDBObject.put(LINKSET_STATUS, linksetStatus);

			insert(checkBeforeInsert);
			return true;
		} catch (Exception e2) {
			// e2.printStackTrace();

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

			downloadedLOV = (Boolean) obj.get(DOWNLOADED_LOV);
			
			linksetTimeStarted = (Date) obj.get(LINKSET_TIME_STARTED);
			
			linksetTimeFinished = (Date) obj.get(LINKSET_TIME_FINISHED);
			
			linksetNextRound = (Date) obj.get(LINKSET_NEXT_ROUND);
			
			linksetStatus = (String) obj.get(LINKSET_STATUS);
			

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
