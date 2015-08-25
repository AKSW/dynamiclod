package dynlod.API.services;

import java.io.IOException;

import org.apache.jena.riot.RiotException;

import dynlod.InputRDFParser;
import dynlod.Manager;
import dynlod.API.core.API;
import dynlod.API.core.APITasks;
import dynlod.exceptions.DynamicLODFormatNotAcceptedException;
import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.exceptions.DynamicLODNoDatasetFoundException;

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
			apiMessage.setCoreMsgSuccess();

			// read and parse description file
			inputRDFParser.readModel(datasetURI, format);
			inputRDFParser.parseDistributions();

			if (inputRDFParser.distributionsLinks.size() > 0) {
				apiMessage.setParserMsg(inputRDFParser.distributionsLinks
						.size()
						+ " distributions found. We are processsing them!");
				
				// stream distributions
				Manager m = new Manager(inputRDFParser.distributionsLinks);
			} else {
				apiMessage.setParserMsg("No datasets found.");

//				APIStatusMongoDBObject apiStatus = new APIStatusMongoDBObject(
//						datasetURI);
//				apiStatus.setMessage("We didn't find any distributions!");

			}
		} catch (DynamicLODNoDatasetFoundException e) {
			apiMessage.setParserMsg(e.getMessage(), true);
			e.printStackTrace();

		} catch (RiotException e) {
			apiMessage.setParserMsg("Bad format file. ", true);
			e.printStackTrace();
		} catch (DynamicLODFormatNotAcceptedException e) {
//			 apiMessage.setParserMsg(e.getMessage(), true);
			 apiMessage.setParserMsg("", true);
			e.printStackTrace();
		} catch (DynamicLODGeneralException e) {
			apiMessage.setParserMsg(e.getMessage(), true);
			e.printStackTrace();
		} catch (IOException e) {
			apiMessage.setParserMsg(e.getMessage(), true);
			e.printStackTrace();
		}

		APITasks.tasks.remove(datasetURI);

	}

	public void run() {
		addDatasets();
	}
}
