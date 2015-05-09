package dataid.API;

import dataid.InputRDFParser;
import dataid.Manager;
import dataid.linksets.MakeLinksets;
import dataid.mongodb.objects.SystemPropertiesMongoDBObject;

public class APIDataset extends API {

	String datasetURI;
	String format;

	public APIDataset(String datasetURI, String format) {
		this.datasetURI = datasetURI;
		this.format = format;
	}

	public void addDatasets() {

		InputRDFParser inputRDFParser = new InputRDFParser();
		try {
			inputRDFParser.readModel(datasetURI, format);
			inputRDFParser.parseDistributions();
			
			if (inputRDFParser.distributionsLinks.size() > 0) {
				addMessage(new APIMessage(true, inputRDFParser.distributionsLinks.size() + " distributions found. We are processsing them!"));
				Manager m = new Manager(inputRDFParser.distributionsLinks);
				System.out.println(inputRDFParser.distributionsLinks.size());
				SystemPropertiesMongoDBObject systemProperties = new SystemPropertiesMongoDBObject();
				systemProperties.setLinksetNeedUpdate(true);
				systemProperties.updateObject(true);
				addMessage(new APIMessage(true, "Done!"));
				
			}
			else{
				addMessage(new APIMessage(false, "We found an error reading your RDF data."));
			}

		} catch (Exception e) {
			addMessage(new APIMessage(false, e.getMessage()));
		}

		addMessage(new APIMessage(true, "Dataset added;"));
		
		APITasks.tasks.remove(datasetURI);

	}

	public void run() {
		addDatasets();
	}
}
