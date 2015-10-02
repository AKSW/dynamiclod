package dynlod.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import dynlod.API.core.APIOption;
import dynlod.API.core.ServiceAPIOptions;
import dynlod.API.services.APIDataset;
import dynlod.API.services.APIFactory;
import dynlod.API.services.APIRetrieveRDF;
import dynlod.API.services.APIStatistics;
import dynlod.API.services.APIStatus;
import dynlod.exceptions.DynamicLODNoDatasetFoundException;
import dynlod.exceptions.api.DynamicLODAPINoLinksFoundException;
import dynlod.exceptions.api.DynamicLODAPINoParametersFoundExceiption;

public class ServiceAPI extends HttpServlet {
	
	final static Logger logger = Logger.getLogger(ServiceAPI.class);

	static HttpServletRequest staticRequest;

	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	public static String getServerURL() {
		return staticRequest.getRequestURL().toString();
	}

	private void manageRequest(HttpServletRequest request,
			HttpServletResponse response) throws IOException {
		staticRequest = request;

		ServiceAPIOptions options = new ServiceAPIOptions();
		PrintWriter out;

		out = response.getWriter();
		try {

			Map<String, String[]> parameters = request.getParameterMap();

			// check whether there is at least one valid parameter
			boolean hasOption = false;
			Iterator<APIOption> it = options.iterator();
			while (it.hasNext()) {
				if (parameters.containsKey(it.next().getOption()))
					hasOption = true;
			}

			if (!hasOption)
				throw new DynamicLODAPINoParametersFoundExceiption();

			if (parameters.containsKey(ServiceAPIOptions.ADD_DATASET)) {
				String format;
				if (parameters.containsKey(ServiceAPIOptions.RDF_FORMAT)) {
					format = (parameters.get(ServiceAPIOptions.RDF_FORMAT)[0].toString());
				} else{
					format = "rdfxml";
				}

				for (String datasetURI : parameters.get(ServiceAPIOptions.ADD_DATASET)) {
					
//					logger.debug("API ADD_DATASET: "+datasetURI+ format);

					APIDataset apiDataset = APIFactory.createDataset(
							datasetURI, format);
					
					while (!apiDataset.apiMessage.hasParserMsg()){
						try {
							Thread.sleep(10);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					out.write(apiDataset.apiMessage.toJSONString());
					out.write("\n");
				}
			}

			if (parameters.containsKey(ServiceAPIOptions.DATASET_STATUS)) {

				for (String datasetURI : parameters.get(ServiceAPIOptions.DATASET_STATUS)) {
					
//					logger.debug("API DATASET_STATUS: "+datasetURI);

					APIStatus apiStatus = APIFactory
							.createStatusDataset(datasetURI);
					try {
						if (apiStatus != null) {
							out.write(apiStatus.apiMessage.toJSONString());
							out.write("\n");
						} else {
							out.write("Error: we couldn't find your dataset. ");
							out.write("\n");
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			if (parameters.containsKey(ServiceAPIOptions.RETRIEVE_DATASET)) {
				for (String datasetURI : parameters
						.get(ServiceAPIOptions.RETRIEVE_DATASET)) {
					APIRetrieveRDF apiRetrieve = null;
					
					if(parameters
						.containsKey("source")  && parameters
						.containsKey("target")){
						apiRetrieve = APIFactory
								.retrieveDataset(parameters
										.get("source")[0],
										parameters
										.get("target")[0]									);
					}
					else
					apiRetrieve = APIFactory
							.retrieveDataset(datasetURI);
					
					apiRetrieve.outModel.write(out, "TURTLE");
				}
			}
			if (parameters.containsKey(ServiceAPIOptions.SERVER_STATISTICS)) {
				out.write(new APIStatistics().getStatistics().toJSONString()); 
			}			
			if (parameters.containsKey(ServiceAPIOptions.LIST)) {
				boolean isVocabulary = false; 
				if(parameters.get(ServiceAPIOptions.LIST_IS_VOCABULARY)[0].toString().equals("true"))
					isVocabulary = true;
				String searchValue = parameters.get(ServiceAPIOptions.LIST_SEARCH)[0];
					
				out.write(new APIStatistics().listDistributions(
						Integer.parseInt(parameters.get(ServiceAPIOptions.LIST_START)[0]),
						Integer.parseInt(parameters.get(ServiceAPIOptions.LIST_SKIP)[0]), isVocabulary,
						searchValue
						).toJSONString()); 
			}
			
			if (parameters.containsKey(ServiceAPIOptions.DATASET_STATISTICS)) {
				out.write(new APIStatistics().datasetDetails(parameters.get(ServiceAPIOptions.DUMP_FILE)[0],
						Integer.parseInt(parameters.get(ServiceAPIOptions.TOP_N)[0]),
						parameters.get(ServiceAPIOptions.TYPE)[0]
						).toJSONString()); 
			}
			if (parameters.containsKey(ServiceAPIOptions.COMPARE_DATASETS)) {
				out.write(new APIStatistics().compareDatasets(Integer.valueOf(parameters.get(ServiceAPIOptions.COMPARE_DATASETS_DATASET1)[0]),
						Integer.valueOf(parameters.get(ServiceAPIOptions.COMPARE_DATASETS_DATASET2)[0]), parameters.get(ServiceAPIOptions.TYPE)[0]).toJSONString()); 
			}	
			
			if (parameters.containsKey(ServiceAPIOptions.DATASET_DETAILS_STATISTICS)) {
				out.write(new APIStatistics().getTop(parameters.get(ServiceAPIOptions.DUMP_FILE)[0],
						Integer.parseInt(parameters.get(ServiceAPIOptions.TOP_N)[0]),
						parameters.get(ServiceAPIOptions.TYPE)[0]
						).toJSONString()); 			
			}	
			
			

		} catch (DynamicLODAPINoParametersFoundExceiption e) {
			Iterator<APIOption> it = options.iterator();
			out.write("We couldn't find any valid parameter.\n\n\n");

			out.write("Parameter \t\t Description\n\n");

			while (it.hasNext()) {
				APIOption o = it.next();
				out.write(o.getOption() + "\t\t" + o.getDescription() + "\n");
			}

			out.write("\n\n\nFor full documentation please access: http://dynamiclod.dbpedia.org/wiki.html");
		} catch (DynamicLODNoDatasetFoundException e) {
			out.write(e.getMessage());
		} catch (DynamicLODAPINoLinksFoundException e) {
			out.write(e.getMessage());

		}
	}
}
