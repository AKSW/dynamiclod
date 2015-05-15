package dynlod.API;

import java.util.ArrayList;
import java.util.HashMap;

import dynlod.mongodb.objects.APIStatusMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.queries.DistributionQueries;

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

		addMessage(new APIMessage("parserMsg", !apiStatus.getHasError(),"Dataset status:  " + apiStatus.getMessage()));			
		for (DistributionMongoDBObject distribution : distributions) {
			boolean success = true; 
			HashMap<String, String> extraMessages = new HashMap<String, String>();
			if(distribution.getStatus().equals(DistributionMongoDBObject.STATUS_ERROR)){
				success = false;
				extraMessages.put(DistributionMongoDBObject.LAST_ERROR_MSG, distribution.getLastErrorMsg());
			}
			extraMessages.put(DistributionMongoDBObject.DOWNLOAD_URL, distribution.getDownloadUrl());
			extraMessages.put(DistributionMongoDBObject.TOP_DATASET, distribution.getTopDataset());
			extraMessages.put(DistributionMongoDBObject.STATUS, distribution.getStatus());
			addMessage(new APIMessage(distribution.getDownloadUrl(),success,
					"Distribution:  "+distribution.getDownloadUrl()+" Status: " +distribution.getStatus()+" "+ distribution.getLastErrorMsg(),
					extraMessages));
		}
//		addMessage(new APIMessage(true,"DONE!"));
		
	}

}
