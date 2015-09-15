package dynlod.mongodb.objects;

import com.mongodb.DBObject;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;

public class PredicateResourceMongoDBObject extends DBSuperClass{
	
	public static final String COLLECTION_NAME = "PredicateResource";

	public static final String PREDICATE_ID = "predicateID";

	public static final String DATASET_ID = "datasetID";

	public static final String DISTRIBUTION_ID = "distributionID";

	
	private int predicateID = 0;

	private int datasetID = 0;

	private int distributionID = 0;

	public PredicateResourceMongoDBObject(int id) {
		super(COLLECTION_NAME, id);
		loadObject();
	}

	public PredicateResourceMongoDBObject(String URI) {
		super(COLLECTION_NAME, URI);
		loadObject();
	}

	@Override
	public boolean updateObject(boolean checkBeforeInsert)
			throws DynamicLODGeneralException {

		// save object case it doens't exists
		try {

			mongoDBObject.put(PREDICATE_ID, predicateID);
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
			
			predicateID = (Integer) obj.get(PREDICATE_ID);
			distributionID = (Integer) obj.get(DISTRIBUTION_ID);
			datasetID = (Integer) obj.get(DATASET_ID);
			
			return true;
		}
		else
			return false;
	}

	public int getPredicateID() {
		return predicateID;
	}

	public void setPredicateID(int predicateID) {
		this.predicateID = predicateID;
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
