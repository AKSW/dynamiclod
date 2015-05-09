package dataid.API;

import dataid.mongodb.objects.APIStatusMongoDBObject;

public class APIFactory {

	public static APIDataset createDataset(String datasetURI, String format) {
		
		if (!APITasks.tasks.containsKey(datasetURI)) {
			APIDataset instace = new APIDataset(datasetURI, format);
			APITasks.tasks.put(datasetURI, instace);
			instace.start();
			return instace;
			
		} else
			return (APIDataset) APITasks.tasks.get(datasetURI);
	}
	
	public static APIStatus createStatusDataset(String datasetURI) {
		APIStatusMongoDBObject apiStatus = new APIStatusMongoDBObject(datasetURI);
		if(apiStatus.getMessage()!=null){
			return new APIStatus(datasetURI);
		}
		else return null;
		
	}

	public static APIRetrieve retrieveDataset(String datasetURI) {
		return new APIRetrieve(datasetURI);

	}

}
