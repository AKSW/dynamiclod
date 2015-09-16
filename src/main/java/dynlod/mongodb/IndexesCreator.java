package dynlod.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import dynlod.mongodb.objects.OWLClassMongoDBObject;
import dynlod.mongodb.objects.OWLClassResourceMongoDBObject;
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.DistributionObjectDomainsMongoDBObject;
import dynlod.mongodb.objects.DistributionSubjectDomainsMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.objects.PredicateMongoDBObject;
import dynlod.mongodb.objects.PredicateResourceMongoDBObject;

public class IndexesCreator {
	
	public void createIndexes(){
		
		// indexes for DistributionObjectDomainsMongoDBObject
		addIndex(DistributionObjectDomainsMongoDBObject.COLLECTION_NAME, DistributionObjectDomainsMongoDBObject.DISTRIBUTION_ID, 1);
		addIndex(DistributionObjectDomainsMongoDBObject.COLLECTION_NAME, DistributionObjectDomainsMongoDBObject.OBJECT_FQDN, 1);
		
		// indexes for DistributionSubjectDomainsMongoDBObject
		addIndex(DistributionSubjectDomainsMongoDBObject.COLLECTION_NAME, DistributionSubjectDomainsMongoDBObject.DISTRIBUTION_ID, 1);
		addIndex(DistributionSubjectDomainsMongoDBObject.COLLECTION_NAME, DistributionSubjectDomainsMongoDBObject.SUBJECT_FQDN, 1);
		
		// indexes for datasets
		addIndex(DatasetMongoDBObject.COLLECTION_NAME, DatasetMongoDBObject.PARENT_DATASETS, 1);
		addIndex(DatasetMongoDBObject.COLLECTION_NAME, DatasetMongoDBObject.TITLE, 1);
		addIndex(DatasetMongoDBObject.COLLECTION_NAME, DatasetMongoDBObject.DYN_LOD_ID, 1);
		addIndex(DatasetMongoDBObject.COLLECTION_NAME, DatasetMongoDBObject.SUBSET_IDS, 1);
		addIndex(DatasetMongoDBObject.COLLECTION_NAME, DatasetMongoDBObject.PARENT_DATASETS, 1);
		
		// indexes for distributions
		addIndex(DistributionMongoDBObject.COLLECTION_NAME, DistributionMongoDBObject.DEFAULT_DATASETS, 1);
		addIndex(DistributionMongoDBObject.COLLECTION_NAME, DistributionMongoDBObject.DOWNLOAD_URL, 1);
		addIndex(DistributionMongoDBObject.COLLECTION_NAME, DistributionMongoDBObject.IS_VOCABULARY, 1);
		addIndex(DistributionMongoDBObject.COLLECTION_NAME, DistributionMongoDBObject.DYN_LOD_ID, 1);
		addIndex(DistributionMongoDBObject.COLLECTION_NAME, DistributionMongoDBObject.DEFAULT_DATASETS, 1);
		
		
		// indexes for linksets
		addIndex(LinksetMongoDBObject.COLLECTION_NAME, LinksetMongoDBObject.DATASET_SOURCE, 1);
		addIndex(LinksetMongoDBObject.COLLECTION_NAME, LinksetMongoDBObject.DATASET_TARGET, 1);
		addIndex(LinksetMongoDBObject.COLLECTION_NAME, LinksetMongoDBObject.DISTRIBUTION_SOURCE, 1);
		addIndex(LinksetMongoDBObject.COLLECTION_NAME, LinksetMongoDBObject.DISTRIBUTION_TARGET, 1);
		
		// indexes for predicates
		addIndex(PredicateMongoDBObject.COLLECTION_NAME, PredicateMongoDBObject.DYN_LOD_ID, 1);
		
		// indexes for predicatesresources
		addIndex(PredicateResourceMongoDBObject.COLLECTION_NAME, PredicateResourceMongoDBObject.PREDICATE_ID, 1);
		addIndex(PredicateResourceMongoDBObject.COLLECTION_NAME, PredicateResourceMongoDBObject.DISTRIBUTION_ID, 1);
		addIndex(PredicateResourceMongoDBObject.COLLECTION_NAME, PredicateResourceMongoDBObject.DATASET_ID, 1);	
		
		// indexes for classes
		addIndex(OWLClassMongoDBObject.COLLECTION_NAME, PredicateMongoDBObject.DYN_LOD_ID, 1);
		
		// indexes for predicatesresources
		addIndex(OWLClassResourceMongoDBObject.COLLECTION_NAME, OWLClassResourceMongoDBObject.CLASS_ID, 1);
		addIndex(OWLClassResourceMongoDBObject.COLLECTION_NAME, OWLClassResourceMongoDBObject.DISTRIBUTION_ID, 1);
		addIndex(OWLClassResourceMongoDBObject.COLLECTION_NAME, OWLClassResourceMongoDBObject.DATASET_ID, 1);
		
	}
	
	public void addIndex(String collection, String field, int value){
		DBObject indexOptions = new BasicDBObject();
		indexOptions.put(field, value);
		DBSuperClass.getInstance().getCollection(collection).createIndex(indexOptions ); 			
		
	}

}
