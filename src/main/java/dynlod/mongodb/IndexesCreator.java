package dynlod.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import dynlod.mongodb.collections.DatasetMongoDBObject;
import dynlod.mongodb.collections.DistributionMongoDBObject;
import dynlod.mongodb.collections.DistributionObjectDomainsMongoDBObject;
import dynlod.mongodb.collections.DistributionSubjectDomainsMongoDBObject;
import dynlod.mongodb.collections.LinksetMongoDBObject;
import dynlod.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import dynlod.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfType.RDFTypeSubjectRelationDB;

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
		addIndex(LinksetMongoDBObject.COLLECTION_NAME, LinksetMongoDBObject.LINK_NUMBER_LINKS, 1);
		addIndex(LinksetMongoDBObject.COLLECTION_NAME, LinksetMongoDBObject.PREDICATE_SIMILARITY, 1);
		addIndex(LinksetMongoDBObject.COLLECTION_NAME, LinksetMongoDBObject.RDF_TYPE_SIMILARITY, 1);
		addIndex(LinksetMongoDBObject.COLLECTION_NAME, LinksetMongoDBObject.RDF_SUBCLASS_SIMILARITY, 1);
		addIndex(LinksetMongoDBObject.COLLECTION_NAME, LinksetMongoDBObject.OWL_CLASS_SIMILARITY, 1);
		addIndex(LinksetMongoDBObject.COLLECTION_NAME, LinksetMongoDBObject.LINK_STRENGHT, 1);
				
		// indexes for predicatesresources
		addIndex(AllPredicatesRelationDB.COLLECTION_NAME, AllPredicatesRelationDB.PREDICATE_ID, 1);
		addIndex(AllPredicatesRelationDB.COLLECTION_NAME, AllPredicatesRelationDB.DISTRIBUTION_ID, 1);
		addIndex(AllPredicatesRelationDB.COLLECTION_NAME, AllPredicatesRelationDB.DATASET_ID, 1);	
				
		// indexes for predicatesresources
		addIndex(RDFTypeObjectRelationDB.COLLECTION_NAME, RDFTypeObjectRelationDB.PREDICATE_ID, 1);
		addIndex(RDFTypeObjectRelationDB.COLLECTION_NAME, RDFTypeObjectRelationDB.DISTRIBUTION_ID, 1);
		addIndex(RDFTypeObjectRelationDB.COLLECTION_NAME, RDFTypeObjectRelationDB.DATASET_ID, 1);
		
		addIndex(RDFTypeSubjectRelationDB.COLLECTION_NAME, RDFTypeSubjectRelationDB.PREDICATE_ID, 1);
		addIndex(RDFTypeSubjectRelationDB.COLLECTION_NAME, RDFTypeSubjectRelationDB.DISTRIBUTION_ID, 1);
		addIndex(RDFTypeSubjectRelationDB.COLLECTION_NAME, RDFTypeSubjectRelationDB.DATASET_ID, 1);
		
		addIndex(RDFSubClassOfRelationDB.COLLECTION_NAME, RDFSubClassOfRelationDB.PREDICATE_ID, 1);
		addIndex(RDFSubClassOfRelationDB.COLLECTION_NAME, RDFSubClassOfRelationDB.DISTRIBUTION_ID, 1);
		addIndex(RDFSubClassOfRelationDB.COLLECTION_NAME, RDFSubClassOfRelationDB.DATASET_ID, 1);
		
		addIndex(OwlClassRelationDB.COLLECTION_NAME, OwlClassRelationDB.PREDICATE_ID, 1);
		addIndex(OwlClassRelationDB.COLLECTION_NAME, OwlClassRelationDB.DISTRIBUTION_ID, 1);
		addIndex(OwlClassRelationDB.COLLECTION_NAME, OwlClassRelationDB.DATASET_ID, 1);
		
		
		
	}
	
	public void addIndex(String collection, String field, int value){
		DBObject indexOptions = new BasicDBObject();
		indexOptions.put(field, value);
		DBSuperClass.getInstance().getCollection(collection).createIndex(indexOptions ); 			
		
	}

}
