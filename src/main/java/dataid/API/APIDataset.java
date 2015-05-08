package dataid.API;

import dataid.InputRDFParser;
import dataid.Manager;
import dataid.linksets.MakeLinksets;

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
				setMessage(new APIMessage(true, inputRDFParser.distributionsLinks.size() + " distributions found. We are processsing them!"));
				Manager m = new Manager(inputRDFParser.distributionsLinks);
				System.out.println(inputRDFParser.distributionsLinks.size());
				MakeLinksets makeLinksets = new MakeLinksets();
				makeLinksets.updateLinksets();
				setMessage(new APIMessage(true,"Done!"));
			}

		} catch (Exception e) {
			setMessage(new APIMessage(false, e.getMessage()));
		}

		setMessage(new APIMessage(true, "Dataset added;"));

	}

	public void run() {
		addDatasets();
	}
}
