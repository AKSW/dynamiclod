package dynlod.API.core;

import java.util.ArrayList;

public class ServiceAPIOptions extends ArrayList<APIOption>{
	
	private static final long serialVersionUID = -7797640635790893518L;

	public static final String ADD_DATASET = "addDataset";
	
	public static final String DATASET_STATUS = "datasetStatus";
	
	public static final String RETRIEVE_DATASET = "retrieveDataset"; 
	
	public static final String RDF_FORMAT = "rdfFormat"; 
	
	public static final String SERVER_STATISTICS = "statistics"; 
	
	public static final String LIST_LINKS = "links"; 
	
	public static final String LIST_DISTRIBUTIONS = "listDistributions"; 
	
	public static final String LIST_SKIP = "skip"; 
	
	public static final String LIST_LIMIT = "limit"; 
	
	{
		add(new APIOption(ADD_DATASET, "link for your dataset description to be streamed. Might be a list of links."));
		add(new APIOption(DATASET_STATUS, "The API parameter used to verify the details of the loading/streaming process for a dataset."));
		add(new APIOption(RETRIEVE_DATASET, "Retrieves RDF data about counted links in the VoID Linkset format."));
		add(new APIOption(RDF_FORMAT, "format of the added links in the addDataset parameter. Formats are: ttl, nt or rdfxml."));
		add(new APIOption(SERVER_STATISTICS, "Retrieve server statistics."));
		add(new APIOption(LIST_DISTRIBUTIONS, "Retruns a list dump files in the server."));
		add(new APIOption(LIST_SKIP, ""));
		add(new APIOption(LIST_LIMIT, ""));
		
	}
	
}
