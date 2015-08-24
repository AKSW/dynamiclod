package dynlod.API.services;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import dynlod.API.core.API;
import dynlod.mongodb.objects.APIStatusMongoDBObject;
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.queries.DatasetQueries;
import dynlod.mongodb.queries.DistributionQueries;
import dynlod.mongodb.queries.LinksetQueries;

public class APIStatus extends API {

	APIStatusMongoDBObject apiStatus = null;
	
	ArrayList<DistributionMongoDBObject> distributions;
	
	final static Logger logger = Logger.getLogger(APIStatus.class);

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	public APIStatus(String url) {
		
//		logger.debug("APIStatus initialized. ");
		
		apiStatus = new APIStatusMongoDBObject(url);
		distributions=DistributionQueries.getDistributionsByTopDatasetID(url);
		
		apiMessage.setCoreMsgSuccess();
//		apiMessage.setParserMsg("Dataset status:  " + apiStatus.getMessage());
		
//		logger.debug("APIStatus number of distributions found: "+distributions.size());
	
		for (DistributionMongoDBObject distribution : distributions) {
			
			JSONObject datasetMessage = new JSONObject();
			
			ArrayList<DatasetMongoDBObject> d = distribution.getDefaultDatasetsAsResources();
			
			Iterator<DatasetMongoDBObject> i = d.iterator();
			
			ArrayList<String> parentNames = new ArrayList<String>();
			while(i.hasNext()){
				parentNames.add(i.next().getUri());
			}
			
			if(distribution.getStatus().equals(DistributionMongoDBObject.STATUS_ERROR)){
				datasetMessage.put(DistributionMongoDBObject.LAST_MSG, distribution.getLastMsg());
			}
			datasetMessage.put(DistributionMongoDBObject.DOWNLOAD_URL, distribution.getDownloadUrl());
			datasetMessage.put(DistributionMongoDBObject.RESOURCE_URI, distribution.getResourceUri()); 
			datasetMessage.put(DistributionMongoDBObject.DEFAULT_DATASETS, parentNames); 
			datasetMessage.put(DistributionMongoDBObject.STATUS, distribution.getStatus());
			datasetMessage.put(DistributionMongoDBObject.TITLE, distribution.getTitle());
			datasetMessage.put(DistributionMongoDBObject.DOWNLOAD_URL, distribution.getDownloadUrl());
			datasetMessage.put(DistributionMongoDBObject.LAST_MSG, distribution.getLastMsg());
			datasetMessage.put(DistributionMongoDBObject.TRIPLES, distribution.getTriples());
			datasetMessage.put(DistributionMongoDBObject.LAST_TIME_STREAMED, distribution.getLastTimeStreamed());
			
			
			// indegrees
			ArrayList<LinksetMongoDBObject> indegrees = new  LinksetQueries().getLinksetsInDegreeByDistribution(distribution.getDynLodID());
			int indegreeCount = 0;
			JSONArray inegreeArray = new JSONArray();
			
			for(LinksetMongoDBObject linkset : indegrees){
				JSONObject indegreeTmpObj = new JSONObject();
				
				// check whether is vocabulary
				DatasetMongoDBObject dataset = new DatasetMongoDBObject(linkset.getDatasetSource()); 
				if(dataset.getIsVocabulary()){
					indegreeTmpObj.put("isVocabulary", true);
				}
				else {
					dataset = new DatasetMongoDBObject(linkset.getDatasetTarget()); 
					if(dataset.getIsVocabulary()){
						indegreeTmpObj.put("isVocabulary", true);
					}
				}
				
				indegreeTmpObj.put("links",linkset.getLinks());
				indegreeTmpObj.put("sourceDataset", new DatasetMongoDBObject(linkset.getDatasetSource()).getUri());
				indegreeTmpObj.put("targetDataset",  new DatasetMongoDBObject(linkset.getDatasetTarget()).getUri());
				indegreeTmpObj.put("sourceDistribution", new DistributionMongoDBObject(linkset.getDistributionSource()).getUri());
				indegreeTmpObj.put("targetDistribution", new DistributionMongoDBObject(linkset.getDistributionTarget()).getUri());
				
				inegreeArray.put(indegreeTmpObj); 
			}
			
			datasetMessage.put("indegree", inegreeArray);
			
//			datasetMessage.put("indegreeDatasetCount", indegrees.size());
//			datasetMessage.put("indegreeLinksCount", indegreeCount);
			
			// outdegrees
			ArrayList<LinksetMongoDBObject> outdegrees = new LinksetQueries().getLinksetsOutDegreeByDistribution(distribution.getDynLodID());
			int outdegreeCount = 0;
			JSONArray outdegreeArray = new JSONArray();
			for(LinksetMongoDBObject linkset : outdegrees){
				JSONObject outdegreeTmpObj = new JSONObject();
				
				// check whether is vocabulary
				DatasetMongoDBObject dataset = new DatasetMongoDBObject(linkset.getDatasetSource()); 
				if(dataset.getIsVocabulary()){
					outdegreeTmpObj.put("isVocabulary", true);
				}
				else {
					dataset = new DatasetMongoDBObject(linkset.getDatasetTarget()); 
					if(dataset.getIsVocabulary()){
						outdegreeTmpObj.put("isVocabulary", true);
					}
				}
				
				outdegreeTmpObj.put("links",linkset.getLinks());
				outdegreeTmpObj.put("sourceDataset", new DatasetMongoDBObject(linkset.getDatasetSource()).getUri());
				outdegreeTmpObj.put("targetDataset",new DatasetMongoDBObject(linkset.getDatasetTarget()).getUri());
				outdegreeTmpObj.put("sourceDistribution",new DistributionMongoDBObject(linkset.getDistributionSource()).getUri());
				outdegreeTmpObj.put("targetDistribution",new DistributionMongoDBObject(linkset.getDistributionTarget()).getUri());
				
				outdegreeArray.put(outdegreeTmpObj); 
			}
			
			datasetMessage.put("outdegree", outdegreeArray);
			
//			datasetMessage.put("outdegreeDatasetCount", outdegrees.size());
//			datasetMessage.put("outdegreeLinksCount", outdegreeCount);	
			
//			logger.debug("APIStatus message: "+ datasetMessage.toString(4));
			
			apiMessage.addDistributionMsg(datasetMessage);
		}
		
	}

}
