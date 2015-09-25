package dynlod.mongodb.collections.RDFResources.rdfSubClassOf;

import java.util.HashMap;
import java.util.HashSet;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;
import dynlod.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;
import dynlod.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;



public class RDFSubClassOfRelationDB extends GeneralRDFResourceRelationDB{

	public static final String COLLECTION_NAME = "RDFSubClassOfRelation";


	public RDFSubClassOfRelationDB(int id) {
		super(COLLECTION_NAME, id);
		loadObject();
	}

	public RDFSubClassOfRelationDB(String URI) {
		super(COLLECTION_NAME, URI);
		loadObject();
	}
	
	public RDFSubClassOfRelationDB() {
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
			RDFSubClassOfDB p = new RDFSubClassOfDB(object);
			try {
				p.updateObject(true);
				RDFSubClassOfRelationDB pr = new RDFSubClassOfRelationDB(p.getDynLodID()+"-"+distributionDynLodID+"-"+topDatasetDynLodID);
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
					RDFSubClassOfRelationDB.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(
					RDFSubClassOfRelationDB.DISTRIBUTION_ID,
					distributionID);

			DBCursor cursor = collection.find(query);
			while (cursor.hasNext()) {
				result.add(((Number) cursor.next().get(
						RDFSubClassOfRelationDB.PREDICATE_ID)).toString());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
