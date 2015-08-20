package dynlod.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import dynlod.mongodb.objects.DistributionObjectDomainsMongoDBObject;
import dynlod.mongodb.objects.DistributionSubjectDomainsMongoDBObject;

public class IndexesCreator {
	
	public void createIndexes(){
		
		// creating database indexes 
		DBObject indexOptions = new BasicDBObject();
		indexOptions.put(DistributionObjectDomainsMongoDBObject.DISTRIBUTION_URI, 1);
		indexOptions.put(DistributionObjectDomainsMongoDBObject.OBJECT_FQDN, 1);
		DBSuperClass.getInstance().getCollection(DistributionObjectDomainsMongoDBObject.COLLECTION_NAME).createIndex(indexOptions );
		
		
		indexOptions = new BasicDBObject();
		indexOptions.put(DistributionSubjectDomainsMongoDBObject.DISTRIBUTION_URI, 1);
		indexOptions.put(DistributionSubjectDomainsMongoDBObject.SUBJECT_FQDN, 1);
		DBSuperClass.getInstance().getCollection(DistributionSubjectDomainsMongoDBObject.COLLECTION_NAME).createIndex(indexOptions );
		
	}

}
