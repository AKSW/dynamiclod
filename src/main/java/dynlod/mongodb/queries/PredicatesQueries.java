package dynlod.mongodb.queries;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;
import dynlod.mongodb.objects.PredicateMongoDBObject;
import dynlod.mongodb.objects.PredicateResourceMongoDBObject;

public class PredicatesQueries {

	/**
	 * Return a set of predicates of distribution
	 * @param distributionID
	 * @return set of string
	 */
	public HashSet<String> getSetOfPredicates(
			int distributionID) {
		
		HashSet<String>  result= new HashSet<String>();
		try {

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					PredicateResourceMongoDBObject.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(
					PredicateResourceMongoDBObject.DISTRIBUTION_ID,
					distributionID);

			DBCursor cursor = collection.find(query);

			while (cursor.hasNext()) {
				result.add(((Number) cursor.next().get(
						PredicateResourceMongoDBObject.PREDICATE_ID)).toString());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * insert a set of predicates of a distribution
	 * @param predicates set of predicates
	 * @param distributionDynLodID dynamiclod id of distribution that contains the predicates
	 * @param topDatasetDynLodID dynamiclod id of top dataset of the distribution that contains the predicates
	 */
	public void insertPredicates(Set<String> predicates, int distributionDynLodID, int topDatasetDynLodID){
		// save predicates
		Iterator<String> i = predicates.iterator();
		while(i.hasNext()){
			String predicate = i.next();
			PredicateMongoDBObject p = new PredicateMongoDBObject(predicate);
			try {
				p.updateObject(true);
				PredicateResourceMongoDBObject pr = new PredicateResourceMongoDBObject(p.getDynLodID()+"-"+distributionDynLodID+"-"+topDatasetDynLodID);
				pr.setDatasetID(topDatasetDynLodID);
				pr.setDistributionID(distributionDynLodID);
				pr.setPredicateID(p.getDynLodID());
				pr.updateObject(true);
			} catch (DynamicLODGeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}
