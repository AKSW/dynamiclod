package dynlod.API.services;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import dynlod.API.core.APIMessage;
import dynlod.API.core.ServiceAPIOptions;
import dynlod.mongodb.collections.DistributionDB;
import dynlod.mongodb.collections.LinksetDB;
import dynlod.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;
import dynlod.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import dynlod.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import dynlod.mongodb.collections.RDFResources.owlClass.OwlClassDB;
import dynlod.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfDB;
import dynlod.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfType.RDFTypeObjectDB;
import dynlod.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import dynlod.mongodb.queries.DatasetQueries;
import dynlod.mongodb.queries.DistributionQueries;
import dynlod.mongodb.queries.LinksetQueries;

public class APIStatistics{
	
	public APIMessage getStatistics(){
		APIMessage apimessage = new APIMessage(); 
		
		JSONObject jsonMsg = new JSONObject();

		// get how many vocabs and datasets are in the database
		int datasets = new DatasetQueries().getDatasetsNotVocab().size();
		
		int vocabularies = new DatasetQueries().getDatasetsVocab().size(); 
		
		int triples = new DistributionQueries().getNumberOfTriples();
		
		NumberFormat formatter = new DecimalFormat("###,###,###,###");

		jsonMsg.put("numberOfDatasets", datasets);
		
		jsonMsg.put("numberOfVocabularies", formatter.format(vocabularies));		
		
		jsonMsg.put("numberOfTriples", formatter.format(triples));

		apimessage.addStatisticsMsg(jsonMsg);
		
		return apimessage;
	}
	
	public APIMessage listDistributions(int skip, int limit, boolean getOntologies, String search){
		APIMessage apimessage = new APIMessage(); 
		
		
		JSONArray jsonArr = new JSONArray();
		JSONObject msg = new JSONObject();

		// get how many vocabs and datasets are in the database
		ArrayList<DistributionDB> distributions = new DistributionQueries()
		.getDistributions(skip, limit, getOntologies, search);
		
		for (DistributionDB d : distributions){
			JSONArray jsonObj = new JSONArray();
			jsonObj.put(d.getTopDatasetTitle());
//			jsonObj.put(new DatasetMongoDBObject(d.getDefaultDatasets().get(0)).getUri());
			jsonObj.put(d.getDownloadUrl());
			jsonObj.put(d.getStatus());
			jsonObj.put(d.getLastTimeStreamed());
			jsonArr.put(jsonObj);
		}
		
		msg.put("distributions", jsonArr);
		msg.put("totalDistributions", new DistributionQueries().countDistributions(getOntologies));

		apimessage.addListMsg(msg); 
		
		return apimessage;
	}

	public APIMessage getTop(String distribution, int topValue, String type){
		APIMessage apimessage = new APIMessage(); 
		NumberFormat formatterLinks = new DecimalFormat("###,###,###,###");
		NumberFormat formatterDecimal = new DecimalFormat("#.####");
		
		JSONArray jsonArr = new JSONArray();
		JSONObject msg = new JSONObject();

		DistributionDB dist= new DistributionDB(distribution);
		
		// get how many vocabs and datasets are in the database
		ArrayList<LinksetDB> links = new LinksetQueries().getLinksetsByDistribution(distribution, topValue, type);
		
		
		for (LinksetDB d : links){
			DistributionDB datasetDB = new DistributionDB(d.getDistributionTarget());
			JSONArray jsonObj = new JSONArray();
//			jsonObj.put(d.getDistributionTarget());
			jsonObj.put(datasetDB.getTitle());
			
			if(type.equals(ServiceAPIOptions.DATASET_TYPE_LINKS))
				jsonObj.put(formatterLinks.format(d.getLinks()));
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_STRENGTH))
				jsonObj.put(formatterDecimal.format(d.getStrength()));
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES))
				jsonObj.put(formatterDecimal.format(d.getOwlClassSimilarity()));
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES))
				jsonObj.put(formatterDecimal.format(d.getRdfSubClassSimilarity()));
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_TYPE))
				jsonObj.put(formatterDecimal.format(d.getRdfTypeSimilarity()));
			else
				jsonObj.put(formatterDecimal.format(d.getPredicateSimilarity()));
			
			jsonObj.put(datasetDB.getDownloadUrl());
			jsonObj.put(datasetDB.getIsVocabulary());
			jsonObj.put(datasetDB.getDynLodID());
			
			jsonArr.put(jsonObj);
		}

		msg.put("distributions", jsonArr);
		msg.put("distributionTitle", dist.getTitle());
		msg.put("distributionID", dist.getDynLodID());
		msg.put("datasetTitle", dist.getTopDatasetTitle());
		apimessage.addListMsg(msg); 
		
		return apimessage;
	}
	
	
//	@Test
//	public void compareDatasets(){
		public APIMessage compareDatasets(int dataset1URL, int dataset2URL, String type){
		
//		String dataset1URL = "https://raw.githubusercontent.com/AKSW/n3-collection/master/Reuters-128.nt";
//		String dataset2URL = "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#";
//		String type = ServiceAPIOptions.DATASET_TYPE_PREDICATES;
		
		
		APIMessage apimessage = new APIMessage(); 
		
		DistributionDB distribution1 = new DistributionDB(dataset1URL);
		DistributionDB distribution2 = new DistributionDB(dataset2URL);
		
		
		HashSet<String> values1 = new HashSet<String>();
		HashSet<String> values2 = new HashSet<String>();
		HashSet<Integer> intersection = new HashSet<Integer>();
		
		JSONArray jsonArr = new JSONArray();
		JSONObject msg = new JSONObject();
		
		String collectionName = "";
		
		if(type.equals(ServiceAPIOptions.DATASET_TYPE_PREDICATES)){
			values1 = new AllPredicatesRelationDB().getSetOfPredicates(distribution1.getDynLodID());
			values2 = new AllPredicatesRelationDB().getSetOfPredicates(distribution2.getDynLodID());
			collectionName = AllPredicatesRelationDB.COLLECTION_NAME;
		}
		else if(type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES)){
			values1 = new OwlClassRelationDB().getSetOfPredicates(distribution1.getDynLodID());
			values2 = new OwlClassRelationDB().getSetOfPredicates(distribution2.getDynLodID());
			collectionName = OwlClassRelationDB.COLLECTION_NAME;
		}
		else if(type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES)){
			values1 = new RDFSubClassOfRelationDB().getSetOfPredicates(distribution1.getDynLodID());
			values2 = new RDFSubClassOfRelationDB().getSetOfPredicates(distribution2.getDynLodID());
			collectionName = RDFSubClassOfRelationDB.COLLECTION_NAME;
		}
		else if(type.equals(ServiceAPIOptions.DATASET_TYPE_TYPE)){
			values1 = new RDFTypeObjectRelationDB().getSetOfPredicates(distribution1.getDynLodID());
			values2 = new RDFTypeObjectRelationDB().getSetOfPredicates(distribution2.getDynLodID());
			collectionName = RDFTypeObjectRelationDB.COLLECTION_NAME;
		}	
	
		intersection = makeIntersecion(values1, values2);

		Set<GeneralRDFResourceRelationDB> h = new GeneralRDFResourceRelationDB().getPredicatesIn
				(collectionName, intersection, distribution1.getDynLodID(), distribution2.getDynLodID());
		
	
		// group by predicate value
		HashMap<String, HashMap<Integer, Integer>> m = new HashMap<String, HashMap<Integer, Integer>>();
		for(GeneralRDFResourceRelationDB value: h){
//			m.put(value.getPredicateID(), value)
		
			int v1 = 0;
			int v2 = 0;
			
			for(GeneralRDFResourceRelationDB value2: h){
				if(value2.getDistributionID() == distribution1.getDynLodID() && value.getPredicateID()  == value2.getPredicateID())
					v1=value2.getAmount();	
			}
			for(GeneralRDFResourceRelationDB value2: h){
				if(value2.getDistributionID() == distribution2.getDynLodID() &&  value.getPredicateID()  == value2.getPredicateID())
					v2=value2.getAmount();	
			}	
			
			if(type.equals(ServiceAPIOptions.DATASET_TYPE_PREDICATES)){
				HashMap<Integer, Integer> hs = new HashMap<Integer, Integer>();
				hs.put(v1, v2);
				AllPredicatesDB a = new AllPredicatesDB(value.getPredicateID());
				m.put(a.getUri(), hs);
			}
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_CLASSES)){
				HashMap<Integer, Integer> hs = new HashMap<Integer, Integer>();
				hs.put(v1, v2);
				OwlClassDB a = new OwlClassDB(value.getPredicateID());
				m.put(a.getUri(), hs);
			}
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_SUBCLASSES)){
				HashMap<Integer, Integer> hs = new HashMap<Integer, Integer>();
				hs.put(v1, v2);
				RDFSubClassOfDB a = new RDFSubClassOfDB(value.getPredicateID());
				m.put(a.getUri(), hs);
			}			
			else if(type.equals(ServiceAPIOptions.DATASET_TYPE_TYPE)){
				HashMap<Integer, Integer> hs = new HashMap<Integer, Integer>();
				hs.put(v1, v2);
				RDFTypeObjectDB a = new RDFTypeObjectDB(value.getPredicateID());
				m.put(a.getUri(), hs);
			}		
			
		}
		
		
		for (String d : m.keySet()){
//			DistributionDB datasetDB = new DistributionDB(d.getDistributionTarget());
			JSONArray jsonObj = new JSONArray();
//			jsonObj.put(d.getDistributionTarget());
			jsonObj.put(d);
			jsonObj.put(m.get(d));
			
			
			
			jsonArr.put(jsonObj);
		}

		msg.put("similarityTableData", jsonArr);
		apimessage.addListMsg(msg); 
//		System.out.println(apimessage.toJSONString());
		
		return apimessage;
	}

	private HashSet<Integer> makeIntersecion(HashSet<String> values1, HashSet<String> values2){
		HashSet<Integer> intersection = new HashSet<Integer>();
	      for (String i : values1) {
	            if(values2.contains(i)) {
	                intersection.add(Integer.valueOf(i));
	            }
	        }
	      
	      return intersection;
	}
	
	
}
