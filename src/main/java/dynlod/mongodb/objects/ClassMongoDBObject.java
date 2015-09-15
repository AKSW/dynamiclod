package dynlod.mongodb.objects;

import com.mongodb.DBObject;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;

public class ClassMongoDBObject extends DBSuperClass {

	public static final String COLLECTION_NAME = "Class";

	public static final String DYN_LOD_ID = "dynLodID";

	private int dynLodID = 0;

	public ClassMongoDBObject(int id) {
		super(COLLECTION_NAME, id);
		loadObject();
	}

	public ClassMongoDBObject(String URI) {
		super(COLLECTION_NAME, URI);
		loadObject();
	}

	@Override
	public boolean updateObject(boolean checkBeforeInsert)
			throws DynamicLODGeneralException {

		// save object case it doens't exists
		try {

			if (dynLodID == 0)
				dynLodID = new DynamicLODCounterMongoDBObject()
						.incrementAndGetID();
			mongoDBObject.put(DYN_LOD_ID, dynLodID);

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
				dynLodID = new DynamicLODCounterMongoDBObject()
						.incrementAndGetID();

			return true;
		}
		else
			return false;
	}

	public int getDynLodID() {
		return dynLodID;
	}

	public void setDynLodID(int dynLodID) {
		this.dynLodID = dynLodID;
	}

}
