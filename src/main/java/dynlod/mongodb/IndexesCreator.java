package dynlod.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import dynlod.mongodb.collections.DatasetDB;
import dynlod.mongodb.collections.DistributionDB;
import dynlod.mongodb.collections.DistributionObjectDomainsDB;
import dynlod.mongodb.collections.DistributionSubjectDomainsDB;
import dynlod.mongodb.collections.LinksetDB;
import dynlod.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import dynlod.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfType.RDFTypeSubjectRelationDB;

public class IndexesCreator {
	
	public void createIndexes(){
		
		// indexes for DistributionObjectDomainsMongoDBObject
		addIndex(DistributionObjectDomainsDB.COLLECTION_NAME, DistributionObjectDomainsDB.DISTRIBUTION_ID, 1);
		addIndex(DistributionObjectDomainsDB.COLLECTION_NAME, DistributionObjectDomainsDB.OBJECT_FQDN, 1);
		
		// indexes for DistributionSubjectDomainsMongoDBObject
		addIndex(DistributionSubjectDomainsDB.COLLECTION_NAME, DistributionSubjectDomainsDB.DISTRIBUTION_ID, 1);
		addIndex(DistributionSubjectDomainsDB.COLLECTION_NAME, DistributionSubjectDomainsDB.SUBJECT_FQDN, 1);
		
		// indexes for datasets
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.PARENT_DATASETS, 1);
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.TITLE, 1);
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.DYN_LOD_ID, 1);
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.SUBSET_IDS, 1);
		addIndex(DatasetDB.COLLECTION_NAME, DatasetDB.PARENT_DATASETS, 1);
		
		// indexes for distributions
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.DEFAULT_DATASETS, 1);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.DOWNLOAD_URL, 1);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.IS_VOCABULARY, 1);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.DYN_LOD_ID, 1);
		addIndex(DistributionDB.COLLECTION_NAME, DistributionDB.DEFAULT_DATASETS, 1);
		
		
		// indexes for linksets
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.DATASET_SOURCE, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.DATASET_TARGET, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.DISTRIBUTION_SOURCE, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.DISTRIBUTION_TARGET, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.LINK_NUMBER_LINKS, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.PREDICATE_SIMILARITY, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.RDF_TYPE_SIMILARITY, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.RDF_SUBCLASS_SIMILARITY, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.OWL_CLASS_SIMILARITY, 1);
		addIndex(LinksetDB.COLLECTION_NAME, LinksetDB.LINK_STRENGHT, 1);
				
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
