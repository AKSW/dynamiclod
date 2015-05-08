package dataid.API;

public class APIFactory {
	
	public static APIDataset createDataset(String datasetURI, String format){
		if (!APITasks.tasks.containsKey(datasetURI)) {
			
			APIDataset instace = new APIDataset(datasetURI, format);
			APITasks.tasks.put(datasetURI, instace);
			instace.start();
			return instace;
		}
		else 
			return (APIDataset) APITasks.tasks.get(datasetURI);
	}

}
