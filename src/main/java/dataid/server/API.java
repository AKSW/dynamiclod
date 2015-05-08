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
import dataid.API.APITasks;
import dataid.linksets.MakeLinksets;

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

						APIDataset apiDataset = APIFactory.createDataset(datasetURI, format);
						out.write(apiDataset.getMessage().toString());

					}

				} else
					out.write("You need specify rdfFormat: \"ttl\", \"rdfxml\" or \"nt\".");

			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}
