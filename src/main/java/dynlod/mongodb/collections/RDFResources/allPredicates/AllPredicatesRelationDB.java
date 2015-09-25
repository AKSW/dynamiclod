package dynlod.mongodb.collections.RDFResources.allPredicates;

import java.util.HashMap;
import java.util.HashSet;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;
import dynlod.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;



public class AllPredicatesRelationDB extends GeneralRDFResourceRelationDB{
	
	public static final String COLLECTION_NAME = "PredicateResource";

	public AllPredicatesRelationDB(int id) {
		super(COLLECTION_NAME, id);
		loadObject();
	}

	public AllPredicatesRelationDB(String URI) {
		super(COLLECTION_NAME, URI);
		loadObject();
	}
	
	public AllPredicatesRelationDB() {
		super();
	}

	@Override
	public void loadLocalVariables() {
	}

	@Override
	public void updateLocalVariables() {		
	}
	
	/**
	 * Store a set of subjects rdf:type values
	 * @param set
	 */
	public void insertSet(HashMap<String, Integer> set, int distributionDynLodID, int topDatasetDynLodID){
		for(String object : set.keySet()){
			AllPredicatesDB p = new AllPredicatesDB(object);
			try {
				p.updateObject(true);
				AllPredicatesRelationDB pr = new AllPredicatesRelationDB(p.getDynLodID()+"-"+distributionDynLodID+"-"+topDatasetDynLodID);
				pr.setDatasetID(topDatasetDynLodID);
				pr.setDistributionID(distributionDynLodID);
				pr.setPredicateID(p.getDynLodID());
				pr.setAmount(set.get(object));
				pr.updateObject(true);
			} catch (DynamicLODGeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Return a set of predicates of distribution
	 * @param distributionID
	 * @return set of string
	 */
	@Override
	public HashSet<String> getSetOfPredicates(
			int distributionID) {
		
		HashSet<String> result = new HashSet<String>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					AllPredicatesRelationDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(
					AllPredicatesRelationDB.DISTRIBUTION_ID,
					distributionID);

			DBCursor cursor = collection.find(query);
			while (cursor.hasNext()) {
				result.add(((Number) cursor.next().get(
						AllPredicatesRelationDB.PREDICATE_ID)).toString());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
}
