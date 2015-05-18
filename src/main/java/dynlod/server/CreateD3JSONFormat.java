package dynlod.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import dynlod.diagram.Bubble;
import dynlod.diagram.Diagram;
import dynlod.diagram.Link;
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.queries.LinksetQueries;

public class CreateD3JSONFormat extends HttpServlet {

	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	public void printOutput(JSONArray nodes, JSONArray links,
			HttpServletResponse response) {

		JSONObject obj = new JSONObject();

		obj.put("nodes", nodes);
		obj.put("links", links);

		try {
			response.getWriter().print(obj);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void manageRequest(HttpServletRequest request,
			HttpServletResponse response) {

		ArrayList<String> nodeList = new ArrayList<String>();

		JSONArray nodes = new JSONArray();
		JSONArray links = new JSONArray();

		Map<String, String[]> parameters = request.getParameterMap();

		if (parameters.containsKey("getAllDistributions")) {

			Diagram diagram = new Diagram();
			ArrayList<LinksetMongoDBObject> linksets = LinksetQueries
					.getLinksetsWithLinks();

			for (LinksetMongoDBObject linkset : linksets) {

				Bubble target = new Bubble(new DistributionMongoDBObject(
						linkset.getSubjectsDistributionTarget()));
				Bubble source = new Bubble(new DistributionMongoDBObject(
						linkset.getObjectsDistributionTarget()));

				Link link = new Link(source, target, linkset.getLinks());

				diagram.addBubble(target);
				diagram.addBubble(source);

				diagram.addLink(link);
			}

			nodes = diagram.getBubblesJSON();
			links = diagram.getLinksJSON();
			printOutput(nodes, links, response);
		}

		if (parameters.containsKey("dataset")) {
			Diagram diagram = new Diagram();
			for (String datasetURI : parameters.get("dataset")) {
				
				datasetURI = datasetURI.replace("@@@@@", "#");
				System.out.println("- - "+datasetURI);

				if (LinksetQueries.checkIfDistributionExists(datasetURI)) {
					
					System.out.println("Distribution: "+ datasetURI);
					
					// get indegree and outdegree for a distribution
					ArrayList<LinksetMongoDBObject> in = LinksetQueries
							.getLinksetsInDegreeByDistribution(datasetURI);
					ArrayList<LinksetMongoDBObject> out = LinksetQueries
							.getLinksetsOutDegreeByDistribution(datasetURI);

					for (LinksetMongoDBObject linkset : in) {
						Bubble target = new Bubble(
								new DistributionMongoDBObject(linkset
										.getSubjectsDistributionTarget()));
						Bubble source = new Bubble(
								new DistributionMongoDBObject(
										linkset.getObjectsDistributionTarget()));

						Link link = new Link(source, target, linkset.getLinks());

						diagram.addBubble(target);
						diagram.addBubble(source);

						diagram.addLink(link);
					}
					// add linksets to jena model
					for (LinksetMongoDBObject linkset : out) {
						Bubble target = new Bubble(
								new DistributionMongoDBObject(linkset
										.getSubjectsDistributionTarget()));
						Bubble source = new Bubble(
								new DistributionMongoDBObject(
										linkset.getObjectsDistributionTarget()));

						Link link = new Link(source, target, linkset.getLinks());

						diagram.addBubble(target);
						diagram.addBubble(source);

						diagram.addLink(link);
					}

				} else if (LinksetQueries.checkIfDatasetExists(datasetURI)) {
					
					System.out.println("Dataset: "+ datasetURI);
					
					// get indegree and outdegree for a distribution
					ArrayList<LinksetMongoDBObject> in = LinksetQueries
							.getLinksetsInDegreeByDataset(datasetURI);
					ArrayList<LinksetMongoDBObject> out = LinksetQueries
							.getLinksetsOutDegreeByDataset(datasetURI);

					// add linksets to jena model
					for (LinksetMongoDBObject linkset : in) {
						Bubble target = new Bubble(
								new DatasetMongoDBObject(linkset
										.getSubjectsDatasetTarget()));
						Bubble source = new Bubble(
								new DatasetMongoDBObject(
										linkset.getObjectsDatasetTarget()));

						Link link = new Link(source, target, linkset.getLinks());

						diagram.addBubble(target);
						diagram.addBubble(source);

						diagram.addLink(link);
					}
					// add linksets to jena model
					for (LinksetMongoDBObject linkset : out) {
						Bubble target = new Bubble(
								new DatasetMongoDBObject(linkset
										.getSubjectsDatasetTarget()));
						Bubble source = new Bubble(
								new DatasetMongoDBObject(
										linkset.getObjectsDatasetTarget()));

						Link link = new Link(source, target, linkset.getLinks());

						diagram.addBubble(target);
						diagram.addBubble(source);

						diagram.addLink(link);
					}
				}
			}
			nodes = diagram.getBubblesJSON();
			links = diagram.getLinksJSON();
			
			printOutput(nodes, links, response);
		}

	}
}
