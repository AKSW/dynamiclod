package dynlod.API.services;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;

import dynlod.API.core.APIMessage;
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.queries.DatasetQueries;
import dynlod.mongodb.queries.DistributionQueries;

public class APIStatistics{
	
	public APIMessage getStatistics(){
		APIMessage apimessage = new APIMessage(); 
		
		JSONObject jsonMsg = new JSONObject();

		// get how many vocabs and datasets are in the database
		int datasets = new DatasetQueries().getDatasetsNotVocab().size();
		
		int vocabularies = new DatasetQueries().getDatasetsVocab().size(); 
		
		int triples = DistributionQueries.getNumberOfTriples();
		
		jsonMsg.put("numberOfDatasets", datasets);
		
		jsonMsg.put("numberOfVocabularies", vocabularies);
		
		jsonMsg.put("numberOfTriples", triples);

		apimessage.addStatisticsMsg(jsonMsg);
		
		return apimessage;
	}
	
	public APIMessage getDistributions(int skip, int limit, boolean getOntologies){
		APIMessage apimessage = new APIMessage(); 
		
		JSONArray jsonArr = new JSONArray();
		JSONObject msg = new JSONObject();

		// get how many vocabs and datasets are in the database
		ArrayList<DistributionMongoDBObject> distributions = DistributionQueries.getDistributions(skip, limit, getOntologies);
		
		for (DistributionMongoDBObject d : distributions){
			JSONArray jsonObj = new JSONArray();
			jsonObj.put(new DatasetMongoDBObject(d.getDefaultDatasets().get(0)).getUri());
			jsonObj.put(d.getDownloadUrl());
			jsonObj.put(d.getStatus());
			jsonObj.put(d.getLastTimeStreamed());
			jsonArr.put(jsonObj);
		}
		
		msg.put("distributions", jsonArr);
		msg.put("totalDistributions", DistributionQueries.countDistributionsByVocabularies(getOntologies));

		apimessage.addListMsg(msg); 
		
		return apimessage;
	}
	
}
