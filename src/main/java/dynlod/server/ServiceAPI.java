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

import dynlod.API.APIDataset;
import dynlod.API.APIFactory;
import dynlod.API.APIOption;
import dynlod.API.APIRetrieve;
import dynlod.API.APIStatistics;
import dynlod.API.APIStatus;
import dynlod.API.ServiceAPIOptions;
import dynlod.exceptions.DynamicLODNoDatasetFoundException;
import dynlod.exceptions.api.DynamicLODAPINoLinksFoundException;
import dynlod.exceptions.api.DynamicLODAPINoParametersFoundExceiption;
import dynlod.lov.LOV;

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

			if (parameters.containsKey(options.ADD_DATASET)) {
				String format;
				if (parameters.containsKey(options.RDF_FORMAT)) {
					format = (parameters.get(options.RDF_FORMAT)[0].toString());
				} else{
					format = "rdfxml";
				}

				for (String datasetURI : parameters.get(options.ADD_DATASET)) {
					
					logger.debug("API ADD_DATASET: "+datasetURI+ format);

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

			if (parameters.containsKey(options.DATASET_STATUS)) {

				for (String datasetURI : parameters.get(options.DATASET_STATUS)) {
					
					logger.debug("API DATASET_STATUS: "+datasetURI);

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

			if (parameters.containsKey(options.RETRIEVE_DATASET)) {
				for (String datasetURI : parameters
						.get(options.RETRIEVE_DATASET)) {
					APIRetrieve apiRetrieve = APIFactory
							.retrieveDataset(datasetURI);
					apiRetrieve.outModel.write(out, "TURTLE");
				}
			}
			if (parameters.containsKey(options.SERVER_STATISTICS)) {
				out.write(new APIStatistics().getStatistics().toJSONString()); 
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
