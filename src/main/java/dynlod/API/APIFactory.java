package dynlod.API;

import org.apache.log4j.Logger;

import dynlod.exceptions.DynamicLODNoDatasetFoundException;
import dynlod.exceptions.api.DynamicLODAPINoLinksFoundException;
import dynlod.mongodb.objects.APIStatusMongoDBObject;

public class APIFactory {
	
	final static Logger logger = Logger.getLogger(APIFactory.class);

	public static APIDataset createDataset(String datasetURI, String format) {
		
		logger.debug("API Factory Create Dataset started");
		
		if (!APITasks.tasks.containsKey(datasetURI)) {
			APIDataset instace = new APIDataset(datasetURI, format);
			APITasks.tasks.put(datasetURI, instace);
			instace.start();
			instace.apiMessage.setCoreMsgSuccess();			
			return instace;
			
		} else
			return (APIDataset) APITasks.tasks.get(datasetURI);
	}
	
	public static APIStatus createStatusDataset(String datasetURI) {
		
		logger.debug("API Factory Create Status Dataset started");
		APIStatusMongoDBObject apiStatus = new APIStatusMongoDBObject(datasetURI);
		if(apiStatus.getMessage()!=null){
			logger.debug("APIStatus instance created");
			return new APIStatus(datasetURI);
		}
		else return null;
		
	}

	public static APIRetrieve retrieveDataset(String datasetURI) throws DynamicLODNoDatasetFoundException, DynamicLODAPINoLinksFoundException {
		return new APIRetrieve(datasetURI);
	}

}
