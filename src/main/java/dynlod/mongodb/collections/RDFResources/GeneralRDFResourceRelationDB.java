package dynlod.mongodb.collections.RDFResources;

import java.util.HashMap;
import java.util.Set;

import com.mongodb.DBObject;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;

public abstract class GeneralRDFResourceRelationDB extends DBSuperClass{
	
	public static final String PREDICATE_ID = "predicateID";

	public static final String DATASET_ID = "datasetID";

	public static final String DISTRIBUTION_ID = "distributionID";
	
	public static final String AMOUNT = "amount";
	
	
	private int predicateID = 0;

	private int datasetID = 0;

	private int distributionID = 0;
	
	private int amount = 0;
	

	public GeneralRDFResourceRelationDB(String collectionName, int dynLodID) {
		super(collectionName, dynLodID);
	}

	public GeneralRDFResourceRelationDB(String collectionName, String id) {
		super(collectionName, id);
	}
	
	public GeneralRDFResourceRelationDB() {
		super();
	}
	
	
	@Override
	public boolean updateObject(boolean checkBeforeInsert)
			throws DynamicLODGeneralException {
		// save object case it doens't exists
		try {
		
			mongoDBObject.put(PREDICATE_ID, predicateID);

			mongoDBObject.put(DATASET_ID, datasetID);

			mongoDBObject.put(DISTRIBUTION_ID, distributionID);

			mongoDBObject.put(AMOUNT, amount);

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
			predicateID = (Integer) obj.get(PREDICATE_ID);

			datasetID = (Integer) obj.get(DATASET_ID);

			distributionID = (Integer) obj.get(DISTRIBUTION_ID);

			amount = (Integer) obj.get(AMOUNT);

			loadLocalVariables();
			
			return true;
		}
		else
			return false;
	}	

	abstract public void loadLocalVariables();
	
	abstract public void updateLocalVariables();

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
	
	
	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	/**
	 * Store a set of rdf:type values
	 * @param set
	 */
	abstract public void insertSet(HashMap<String, Integer> set, int distributionID, int datasetID);
	
	public abstract Set<String> getSetOfPredicates(int distributionDynLODID);
	
}