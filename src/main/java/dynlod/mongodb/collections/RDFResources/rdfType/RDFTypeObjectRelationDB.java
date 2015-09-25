package dynlod.mongodb.collections.RDFResources.rdfType;

import java.util.HashMap;
import java.util.HashSet;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;
import dynlod.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;



public class RDFTypeObjectRelationDB extends GeneralRDFResourceRelationDB{

	public static final String COLLECTION_NAME = "RDFTypeObjectsRelation";


	public RDFTypeObjectRelationDB(int id) {
		super(COLLECTION_NAME, id);
		loadObject();
	}

	public RDFTypeObjectRelationDB(String URI) {
		super(COLLECTION_NAME, URI);
		loadObject();
	}
	
	public RDFTypeObjectRelationDB() {
		super();
	}

	@Override
	public void loadLocalVariables() {
	}

	@Override
	public void updateLocalVariables() {		
	}
	

	/**
	 * Store a set of object rdf:type values
	 * @param set
	 */
	public void insertSet(HashMap<String, Integer> set, int distributionDynLodID, int topDatasetDynLodID){
		for(String object : set.keySet()){
			RDFTypeObjectDB p = new RDFTypeObjectDB(object);
			try {
				p.updateObject(true);
				RDFTypeObjectRelationDB pr = new RDFTypeObjectRelationDB(p.getDynLodID()+"-"+distributionDynLodID+"-"+topDatasetDynLodID);
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
					RDFTypeObjectRelationDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(
					RDFTypeObjectRelationDB.DISTRIBUTION_ID,
					distributionID);

			DBCursor cursor = collection.find(query);
			while (cursor.hasNext()) {
				result.add(((Number) cursor.next().get(
						RDFTypeObjectRelationDB.PREDICATE_ID)).toString());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
