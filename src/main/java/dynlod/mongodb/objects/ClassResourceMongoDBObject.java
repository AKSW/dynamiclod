package dynlod.mongodb.objects;

import com.mongodb.DBObject;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;

public class ClassResourceMongoDBObject extends DBSuperClass{

	public static final String COLLECTION_NAME = "ClassResource";

	public static final String CLASS_ID = "classID";

	public static final String DATASET_ID = "datasetID";

	public static final String DISTRIBUTION_ID = "distributionID";

	
	private int classID = 0;

	private int datasetID = 0;

	private int distributionID = 0;

	public ClassResourceMongoDBObject(int id) {
		super(COLLECTION_NAME, id);
		loadObject();
	}

	public ClassResourceMongoDBObject(String URI) {
		super(COLLECTION_NAME, URI);
		loadObject();
	}

	@Override
	public boolean updateObject(boolean checkBeforeInsert)
			throws DynamicLODGeneralException {

		// save object case it doens't exists
		try {

			mongoDBObject.put(CLASS_ID, classID);
			mongoDBObject.put(DATASET_ID, datasetID);
			mongoDBObject.put(DISTRIBUTION_ID, distributionID);

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
			
			classID = (Integer) obj.get(CLASS_ID);
			distributionID = (Integer) obj.get(DISTRIBUTION_ID);
			datasetID = (Integer) obj.get(DATASET_ID);
			
			return true;
		}
		else
			return false;
	}


	public int getClassID() {
		return classID;
	}

	public void setClassID(int classID) {
		this.classID = classID;
	}

	public int getDatasetID() {
		return datasetID;
	}

	public void setDatasetID(int datasetID) {
		this.datasetID = datasetID;
	}

	public int getDistributionID() {
		return distributionID;
	}

	public void setDistributionID(int distributionID) {
		this.distributionID = distributionID;
	}
	

}
