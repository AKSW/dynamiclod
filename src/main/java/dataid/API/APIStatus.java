package dataid.API;

import java.util.ArrayList;

import dataid.mongodb.objects.APIStatusMongoDBObject;
import dataid.mongodb.objects.DistributionMongoDBObject;
import dataid.mongodb.queries.DistributionQueries;

public class APIStatus extends API {

	APIStatusMongoDBObject apiStatus = null;
	
	ArrayList<DistributionMongoDBObject> distributions;

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	public APIStatus(String url) {
		apiStatus = new APIStatusMongoDBObject(url);
		distributions=DistributionQueries.getDistributionsByTopDataset(url);
		addMessage(new APIMessage(!apiStatus.getHasError(),"Dataset status:  "+apiStatus.getMessage()));			
		for (DistributionMongoDBObject distribution : distributions) {
			addMessage(new APIMessage(false,"Distribution:  "+distribution.getDownloadUrl()+" Status: " +distribution.getStatus()+" "+ distribution.getLastErrorMsg()));
		}
		addMessage(new APIMessage(false,"DONE!"));
		
	}

}
