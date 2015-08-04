package dynlod.API;

import dynlod.InputRDFParser;
import dynlod.Manager;
import dynlod.linksets.MakeLinksets;
import dynlod.mongodb.objects.APIStatusMongoDBObject;
import dynlod.mongodb.objects.SystemPropertiesMongoDBObject;

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
			apimessage.setCoreMsgSuccess();
			
			inputRDFParser.readModel(datasetURI, format);
			inputRDFParser.parseDistributions();
			
			if (inputRDFParser.distributionsLinks.size() > 0) {
//				addMessage(new APIMessage("parserMsg",true, inputRDFParser.distributionsLinks.size() + " distributions found. We are processsing them!"));
				apimessage.setParserMsg(inputRDFParser.distributionsLinks.size() + " distributions found. We are processsing them!");
				Manager m = new Manager(inputRDFParser.distributionsLinks);
				System.out.println(inputRDFParser.distributionsLinks.size());
//				addMessage(new APIMessage(true, "Done!"));
				
			}
			else{
//				addMessage(new APIMessage("parserMsg",false, "We found an error reading your RDF data."));
				apimessage.setParserMsg("We found an error reading your RDF data."); 
				
				APIStatusMongoDBObject apiStatus = new APIStatusMongoDBObject(datasetURI);
				apiStatus.setMessage("We didn't find any distributions!");
				
			}

		} catch (Exception e) {
//			addMessage(new APIMessage("parserMsg",false, e.getMessage()));
			apimessage.setParserMsg(e.getMessage());
			
			e.printStackTrace();
		}
		
		APITasks.tasks.remove(datasetURI);

	}

	public void run() {
		addDatasets();
	}
}
