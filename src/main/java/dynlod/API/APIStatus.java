package dynlod.API;

import java.util.ArrayList;
import java.util.HashMap;

import org.json.JSONObject;

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
		
		apimessage.setCoreMsgSuccess();
		apimessage.setParserMsg("Dataset status:  " + apiStatus.getMessage());

//		addMessage(new APIMessage("parserMsg", !apiStatus.getHasError(),"Dataset status:  " + apiStatus.getMessage()));			
		for (DistributionMongoDBObject distribution : distributions) {
			
			JSONObject tmp = new JSONObject();
			boolean success = true; 
			
			if(distribution.getStatus().equals(DistributionMongoDBObject.STATUS_ERROR)){
				success = false;
				tmp.put(DistributionMongoDBObject.LAST_ERROR_MSG, distribution.getLastErrorMsg());
			}
			tmp.put(DistributionMongoDBObject.DOWNLOAD_URL, distribution.getDownloadUrl());
			tmp.put(DistributionMongoDBObject.TOP_DATASET, distribution.getTopDataset());
			tmp.put(DistributionMongoDBObject.STATUS, distribution.getStatus());
			apimessage.addDistributionMsg(tmp);
//			addMessage(new APIMessage(distribution.getDownloadUrl(),success,
//					"Distribution:  "+distribution.getDownloadUrl()+" Status: " +distribution.getStatus()+" "+ distribution.getLastErrorMsg(),
//					extraMessages));
		}
//		addMessage(new APIMessage(true,"DONE!"));
		
	}

}
