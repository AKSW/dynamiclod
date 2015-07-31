package dynlod.API;

import java.util.ArrayList;

public class ServiceAPIOptions extends ArrayList<APIOption>{
	
	public String ADD_DATASET = "addDataset";
	
	public String DATASET_STATUS = "datasetStatus";
	
	public String RETRIEVE_DATASET = "retrieveDataset"; 
	
	public String RDF_FORMAT = "rdfFormat"; 
	
	{
		add(new APIOption(ADD_DATASET, "link for your dataset description to be streamed. Might be a list of links."));
		add(new APIOption(DATASET_STATUS, "The API parameter used to verify the details of the loading/streaming process for a dataset."));
		add(new APIOption(RETRIEVE_DATASET, "Retrieves RDF data about counted links in the VoID Linkset format."));
		add(new APIOption(RDF_FORMAT, "format of the added links in the addDataset parameter. Formats are: ttl, nt or rdfxml."));
	}
	
}
