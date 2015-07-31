package dynlod.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import dynlod.API.APIDataset;
import dynlod.API.APIFactory;
import dynlod.API.APIOption;
import dynlod.API.APIRetrieve;
import dynlod.API.APIStatus;
import dynlod.API.ServiceAPIOptions;

public class ServiceAPI extends HttpServlet {

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
			HttpServletResponse response) {
		staticRequest = request;

		PrintWriter out;
		try {
			out = response.getWriter();

			ServiceAPIOptions options = new ServiceAPIOptions();

			Map<String, String[]> parameters = request.getParameterMap();

			// check whether there is at least one valid parameter
			boolean hasOption = false;
			Iterator<APIOption> it = options
					.iterator();
			while (it.hasNext()) {
				if (parameters.containsKey(it.next().getOption()))
					hasOption = true;
			}

			if (!hasOption) {
				it =  options.iterator();
				out.write("We couldn't find any valid parameter.\n\n\n");
				
				out.write("Parameter \t\t Description\n\n");
				
				while (it.hasNext()) {
					APIOption o = it.next();
					out.write(o.getOption() + "\t\t" + o.getDescription()+"\n");
				}
			}
			
			

			if (parameters.containsKey(options.ADD_DATASET)) {
				if (parameters.containsKey(options.RDF_FORMAT)) {
					String format = (parameters.get(options.ADD_DATASET)[0]
							.toString());

					for (String datasetURI : parameters
							.get(options.ADD_DATASET)) {

						APIDataset apiDataset = APIFactory.createDataset(
								datasetURI, format);
						out.write(apiDataset.getMessageJSON().toString());
						out.write("\n");

					}

				} else
					out.write("You need specify rdfFormat: \"ttl\", \"rdfxml\" or \"nt\".");
			}

			if (parameters.containsKey(options.DATASET_STATUS)) {

				for (String datasetURI : parameters.get(options.DATASET_STATUS)) {

					APIStatus apiStatus = APIFactory
							.createStatusDataset(datasetURI);
					try {
						if (apiStatus != null) {
							out.write(apiStatus.getMessageJSON());
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

		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}
