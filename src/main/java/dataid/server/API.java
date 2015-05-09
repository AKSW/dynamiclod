package dataid.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import dataid.API.APIDataset;
import dataid.API.APIFactory;
import dataid.API.APIRetrieve;
import dataid.API.APIStatus;
import dataid.API.APITasks;
import dataid.linksets.MakeLinksets;
import dataid.mongodb.objects.APIStatusMongoDBObject;

public class API extends HttpServlet {

	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	private void manageRequest(HttpServletRequest request,
			HttpServletResponse response) {

		PrintWriter out;
		try {
			out = response.getWriter();

			Map<String, String[]> parameters = request.getParameterMap();

			if (parameters.containsKey("makeLinksets")) {
				String[] makeLinkset = parameters.get("makeLinksets");
				if (makeLinkset[0].toString().equals("true")) {
					MakeLinksets m = new MakeLinksets();
					m.updateLinksets();
				}
			}
			if (parameters.containsKey("addDataset")) {
				if (parameters.containsKey("rdfFormat")) {
					String format = (parameters.get("rdfFormat")[0].toString());

					for (String datasetURI : parameters.get("addDataset")) {

						APIDataset apiDataset = APIFactory.createDataset(
								datasetURI, format);
						out.write(apiDataset.getMessage().toString());
						out.write("<br>");

					}

				} else
					out.write("You need specify rdfFormat: \"ttl\", \"rdfxml\" or \"nt\".");
			}

			if (parameters.containsKey("datasetStatus")) {

				for (String datasetURI : parameters.get("datasetStatus")) {

					APIStatus apiStatus = APIFactory.createStatusDataset(datasetURI);
					if (apiStatus!=null) {
						out.write(apiStatus.getMessage().toString());
						out.write("\n");
					}
					else{
						out.write("Error: we couldn't find your dataset. ");
						out.write("\n");
					}
						
						

				}

			}

			// if (parameters.containsKey("retrieveDataset")) {
			//
			// for (String datasetURI : parameters.get("retrieveDataset")) {
			//
			// APIRetrieve apiDataset = APIFactory.retrieveDataset(datasetURI);
			// out.write(apiDataset.getMessage().toString());
			// out.write("<br>");
			//
			// }
			//
			// }

		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}
