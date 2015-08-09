package dynlod.API;

import java.util.ArrayList;
import java.util.HashMap;

import org.json.JSONObject;

import dynlod.mongodb.objects.APIStatusMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.queries.DistributionQueries;
import dynlod.mongodb.queries.LinksetQueries;

public class APIStatus extends API {

	APIStatusMongoDBObject apiStatus = null;
	
	ArrayList<DistributionMongoDBObject> distributions;

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	public APIStatus(String url) {
		apiStatus = new APIStatusMongoDBObject(url);
		distributions=DistributionQueries.getDistributionsByTopDatasetAccessURL(url);
		
		apiMessage.setCoreMsgSuccess();
//		apiMessage.setParserMsg("Dataset status:  " + apiStatus.getMessage());
	
		for (DistributionMongoDBObject distribution : distributions) {
			
			JSONObject datasetMessage = new JSONObject();
			
			if(distribution.getStatus().equals(DistributionMongoDBObject.STATUS_ERROR)){
				datasetMessage.put(DistributionMongoDBObject.LAST_MSG, distribution.getLastMsg());
			}
			datasetMessage.put(DistributionMongoDBObject.DOWNLOAD_URL, distribution.getDownloadUrl());
			datasetMessage.put(DistributionMongoDBObject.RESOURCE_URI, distribution.getResourceUri()); 
			datasetMessage.put(DistributionMongoDBObject.DEFAULT_DATASETS, distribution.getDefaultDatasets()); 
			datasetMessage.put(DistributionMongoDBObject.STATUS, distribution.getStatus());
			datasetMessage.put(DistributionMongoDBObject.TITLE, distribution.getTitle());
			datasetMessage.put(DistributionMongoDBObject.DOWNLOAD_URL, distribution.getDownloadUrl());
			datasetMessage.put(DistributionMongoDBObject.LAST_MSG, distribution.getLastMsg());
			datasetMessage.put(DistributionMongoDBObject.TRIPLES, distribution.getTriples());
			
			
			// indegrees
			ArrayList<LinksetMongoDBObject> indegrees = LinksetQueries.getLinksetsInDegreeByDistribution(distribution.getDownloadUrl());
			int indegreeCount = 0;
			for(LinksetMongoDBObject m : indegrees){
				indegreeCount = indegreeCount + m.getLinks();
			}
			
			datasetMessage.put("indegreeDatasetCount", indegrees.size());
			datasetMessage.put("indegreeLinksCount", indegreeCount);
			
			// outdegrees
			ArrayList<LinksetMongoDBObject> outdegrees = LinksetQueries.getLinksetsOutDegreeByDistribution(distribution.getDownloadUrl());
			int outdegreeCount = 0;
			for(LinksetMongoDBObject m : outdegrees){
				outdegreeCount = outdegreeCount + m.getLinks();
			}
			
			datasetMessage.put("outdegreeDatasetCount", outdegrees.size());
			datasetMessage.put("outdegreeLinksCount", outdegreeCount);			
			
			apiMessage.addDistributionMsg(datasetMessage);
		}
		
	}

}
