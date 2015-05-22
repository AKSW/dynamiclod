package dynlod.mongodb.objects;

import java.util.Date;

import com.mongodb.DBObject;

import dynlod.exceptions.DataIDException;
import dynlod.mongodb.DataIDDB;

public class SystemPropertiesMongoDBObject extends DataIDDB {

	// Collection name
	public static final String COLLECTION_NAME = "systemProperties";

	public static final String DOWNLOADED_LOV = "downloadedLOV";

	public static final String LINKSET_TIME_STARTED = "linksetTimeStarted";

	public static final String LINKSET_TIME_FINISHED = "linksetTimeFinished";

	public static final String LINKSET_STATUS = "linksetStatus";

	public static final String LINKSET_NEED_UPDATE = "linksetNeedUpdate";

	// class properties

	private Boolean downloadedLOV;
	
	private Date linksetTimeStarted; 
	
	private Date linksetTimeFinished; 
	
	private String linksetStatus;
	
	private Boolean linksetNeedUpdate;
	

	public SystemPropertiesMongoDBObject() {
		super(COLLECTION_NAME, COLLECTION_NAME);
		loadObject();
	}

	public boolean updateObject(boolean checkBeforeInsert) {
		try {
			mongoDBObject.put(DOWNLOADED_LOV, downloadedLOV);

			mongoDBObject.put(LINKSET_TIME_STARTED, linksetTimeStarted);

			mongoDBObject.put(LINKSET_TIME_FINISHED, linksetTimeFinished);

			mongoDBObject.put(LINKSET_STATUS, linksetStatus);

			mongoDBObject.put(LINKSET_NEED_UPDATE, linksetNeedUpdate);

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
			
			linksetStatus = (String) obj.get(LINKSET_STATUS);
			
			linksetNeedUpdate = (Boolean) obj.get(LINKSET_NEED_UPDATE);
			
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

	public Date getLinksetTimeStarted() {
		return linksetTimeStarted;
	}

	public void setLinksetTimeStarted(Date linksetTimeStarted) {
		this.linksetTimeStarted = linksetTimeStarted;
	}

	public Date getLinksetTimeFinished() {
		return linksetTimeFinished;
	}

	public void setLinksetTimeFinished(Date linksetTimeFinished) {
		this.linksetTimeFinished = linksetTimeFinished;
	}

	public String getLinksetStatus() {
		return linksetStatus;
	}

	public void setLinksetStatus(String linksetStatus) {
		this.linksetStatus = linksetStatus;
	}

	public Boolean getLinksetNeedUpdate() {
		return linksetNeedUpdate;
	}

	public void setLinksetNeedUpdate(Boolean linksetNeedUpdate) {
		this.linksetNeedUpdate = linksetNeedUpdate;
	}
	
	

}
