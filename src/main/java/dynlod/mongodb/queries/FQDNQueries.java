package dynlod.mongodb.queries;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import dynlod.mongodb.DBSuperClass;
import dynlod.mongodb.collections.DistributionObjectDomainsMongoDBObject;

public class FQDNQueries {
	public int getNumberOfObjectResources(
			int distributionID) {
		
		int result=0;
		try {

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionObjectDomainsMongoDBObject.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(
					DistributionObjectDomainsMongoDBObject.DISTRIBUTION_ID,
					distributionID);

			DBCursor cursor = collection.find(query);

			while (cursor.hasNext()) {
				result = result + ((Number) cursor.next().get(
						DistributionObjectDomainsMongoDBObject.NUMBER_OF_RESOURCES)).intValue();
			}

			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	} 
}
