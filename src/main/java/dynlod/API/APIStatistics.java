package dynlod.API;

import org.json.JSONObject;

import dynlod.mongodb.queries.DatasetQueries;

public class APIStatistics{
	
	private String DATASET_KEY= "numberOfDatasets";

	private String VOCABULARY_KEY= "numberOfVocabularies";

	
	public APIMessage getStatistics(){
		APIMessage apimessage = new APIMessage(); 
		
		JSONObject jsonMsg = new JSONObject();

		// get how many vocabs and datasets are in the database
		int datasets = DatasetQueries.getDatasetsNotVocab().size();
		
		int vocabularies = DatasetQueries.getDatasetsVocab().size(); 
		
		jsonMsg.put(DATASET_KEY, datasets);
		
		jsonMsg.put(VOCABULARY_KEY, vocabularies);
		
		apimessage.addStatisticsMsg(jsonMsg);
		
		
		
		return apimessage;
	}
	
}
