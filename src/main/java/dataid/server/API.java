package dataid.server;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import dataid.FileInputParser;
import dataid.Manager;
import dataid.mongodb.actions.MakeLinksets;

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

		Map parameters = request.getParameterMap();

		if (parameters.containsKey("makeLinksets")) {
			if (parameters.get("makeLinksets").equals("true")) {
				MakeLinksets m = new MakeLinksets();
				m.updateLinksets();
			}
		}
		if (parameters.containsKey("addDataset")) {
			for (String parameter : request.getParameterValues("addDataset")) {
				FileInputParser f = new FileInputParser();
				try {
					f.readModel(parameter);
					f.parseDistributions();
					if (f.distributionsLinks.size() > 0) {
					Manager m = new Manager(f.distributionsLinks);
						System.out.println(f.distributionsLinks.size());
						MakeLinksets makeLinksets = new MakeLinksets();
						makeLinksets.updateLinksets();
					}
					else
						System.out.println("Naos");

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}
		System.out.println(parameters);

	}

}
