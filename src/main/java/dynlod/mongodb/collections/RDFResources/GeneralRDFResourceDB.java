package dynlod.mongodb.collections.RDFResources;

import java.util.Set;

import com.mongodb.DBObject;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;
import dynlod.mongodb.collections.DynamicLODCounterDB;

public abstract class GeneralRDFResourceDB extends DBSuperClass  {

	public GeneralRDFResourceDB(String collectionName, int dynLodID) {
		super(collectionName, dynLodID);
		this.dynLodID = dynLodID;
		mongoDBObject.put(DYN_LOD_ID, dynLodID);
		loadObject();
	}

	public GeneralRDFResourceDB(String collectionName, String id) {
		super(collectionName, id);
		loadObject();
	}
	
	public GeneralRDFResourceDB() {
		super();
	}

	public final String DYN_LOD_ID = "dynLodID";

	protected int dynLodID = 0;
	

	public int getDynLodID() {
		return dynLodID;
	}

	public void setDynLodID(int dynLodID) {
		this.dynLodID = dynLodID;
	}
	
	@Override
	public boolean updateObject(boolean checkBeforeInsert)
			throws DynamicLODGeneralException {
		// save object case it doens't exists
		try {
			if (dynLodID == 0)
				dynLodID = new DynamicLODCounterDB()
						.incrementAndGetID();
			mongoDBObject.put(DYN_LOD_ID, dynLodID);

			updateLocalVariables();
			
			insert(checkBeforeInsert);
			return true;

		} catch (Exception e2) {
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

	@Override
	protected boolean loadObject() {
		DBObject obj = search();
		if (obj != null) {
			// mongoDBObject = (BasicDBObject) obj;
			uri = (String) obj.get(URI);
			dynLodID = (Integer) obj.get(DYN_LOD_ID);
			if (dynLodID == 0)
				dynLodID = new DynamicLODCounterDB()
						.incrementAndGetID();
			loadLocalVariables();
			return true;
		}
		else
			return false;
	}	

	abstract public void loadLocalVariables();
	
	abstract public void updateLocalVariables();
	
	abstract public void insertSet(Set<String> set);
	

	
}
