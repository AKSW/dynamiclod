package dynlod.API;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;

import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.queries.DatasetQueries;
import dynlod.mongodb.queries.DistributionQueries;

public class APIStatistics{
	
	public APIMessage getStatistics(){
		APIMessage apimessage = new APIMessage(); 
		
		JSONObject jsonMsg = new JSONObject();

		// get how many vocabs and datasets are in the database
		int datasets = DatasetQueries.getDatasetsNotVocab().size();
		
		int vocabularies = DatasetQueries.getDatasetsVocab().size(); 
		
		int triples = DistributionQueries.getNumberOfTriples();
		
		jsonMsg.put("numberOfDatasets", datasets);
		
		jsonMsg.put("numberOfVocabularies", vocabularies);
		
		jsonMsg.put("numberOfTriples", triples);

		apimessage.addStatisticsMsg(jsonMsg);
		
		return apimessage;
	}
	
	public APIMessage getDistributions(int skip, int limit){
		APIMessage apimessage = new APIMessage(); 
		
		JSONArray jsonArr = new JSONArray();
		JSONObject msg = new JSONObject();

		// get how many vocabs and datasets are in the database
		ArrayList<DistributionMongoDBObject> distributions = DistributionQueries.getDistributions(skip, limit);
		
		for (DistributionMongoDBObject d : distributions){
			JSONArray jsonObj = new JSONArray();
			jsonObj.put(d.getDownloadUrl());
			jsonObj.put(d.getDefaultDatasets().get(0));
			jsonObj.put(d.getStatus());
			jsonObj.put(d.getLastTimeStreamed());
			jsonArr.put(jsonObj);
		}
		
		msg.put("distributions", jsonArr);
		msg.put("totalDistributions", DistributionQueries.getNumberOfDistributions());
		
		
//		for (DistributionMongoDBObject d : distributions){
//			JSONObject jsonObj = new JSONObject();
//			jsonObj.put(DistributionMongoDBObject.DOWNLOAD_URL, d.getDownloadUrl());
//			jsonObj.put(DistributionMongoDBObject.DEFAULT_DATASETS, d.getDefaultDatasets().get(0));
//			jsonObj.put(DistributionMongoDBObject.STATUS, d.getStatus());
//			jsonObj.put(DistributionMongoDBObject.LAST_TIME_STREAMED, d.getLastTimeStreamed());
//			jsonArr.put(jsonObj);
//		}
		
		

		apimessage.addListMsg(msg); 
		
		return apimessage;
	}
	
}
