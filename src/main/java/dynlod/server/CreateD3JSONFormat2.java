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

public class CreateD3JSONFormat2 extends HttpServlet {

	
	boolean showDistribution = true;
	
	boolean showOntologies = false;
	

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

		JSONArray nodes = new JSONArray();
		JSONArray links = new JSONArray();

		Map<String, String[]> parameters = request.getParameterMap();

		if (parameters.containsKey("showDistributions")) 
			showDistribution = true;
		else
			showDistribution = false;
			
		

		if (parameters.containsKey("showOntologies")) 
			showOntologies = true;
		else
			showOntologies = false;
		
		
		if (parameters.containsKey("getAllDistributions")) {

			Diagram diagram = new Diagram();
			ArrayList<LinksetMongoDBObject> linksets = LinksetQueries
					.getLinksetsWithLinks();

			for (LinksetMongoDBObject linkset : linksets) {

				Bubble target = new Bubble(new DistributionMongoDBObject(
						linkset.getDistributionTarget()));
				Bubble source = new Bubble(new DistributionMongoDBObject(
						linkset.getDistributionSource()));

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
			Diagram diagramTemp = new Diagram();
			for (String datasetURI : parameters.get("dataset")) {

				int currentLevel = 1;
//				if (parameters.containsKey("level")) {
//					currentLevel = Integer.parseInt(parameters.get("level")[0]);
//				}

				iterateDataset(datasetURI, diagramTemp, datasetURI, currentLevel);				
			}
			
			Diagram diagram = new Diagram();
			
//			for (Bubble bubble : diagramTemp.getBubbles()) {
//				if(bubble.isVisible())
//					diagram.addBubble(bubble);
//			}
			
//			for (Link link : diagramTemp.getLinks()) {
//				
//				if(link.getSource().isVisible() && link.getTarget().isVisible()){
//					makeLink0(link.getSource(), link.getTarget(), diagram, diagramTemp.getPathWeight(link.getSource().getName(), link.getTarget().getName())); 
//				}
//				else
//				for(String s: link.getSource().getParentDataset()){
//					for(String t: link.getTarget().getParentDataset()){			
//					makeLink0(new Bubble(new DatasetMongoDBObject(s)), 
//							new Bubble(new DatasetMongoDBObject(t)), diagram, 
//							diagramTemp.getPathWeight(s, t)); 
//					System.out.println(diagramTemp.getPathWeight(s, t));
//					}
//				}
//				
//			}

			
			diagramTemp.printSelectedBubbles(parameters.get("dataset"));
			
			nodes = diagramTemp.getBubblesJSON();
			links = diagramTemp.getLinksJSON();

			printOutput(nodes, links, response);
		}

	}

	private void iterateDataset(String datasetURI,
			Diagram diagram, String lastParentDataset, int currentLevel) {
		DatasetMongoDBObject d = new DatasetMongoDBObject(datasetURI);

		
		
		for (String subset : d.getSubsetsURIs()) {
			
		
			makeLink0(new Bubble(new DatasetMongoDBObject(datasetURI), true,lastParentDataset),
					  new Bubble(new DatasetMongoDBObject(subset), true,lastParentDataset), diagram, 0);				
			iterateDataset(subset, diagram, lastParentDataset, --currentLevel);
		}

		for (String distributionURI : d.getDistributionsURIs()) {
			

			makeLink0(new Bubble(new DatasetMongoDBObject(datasetURI), true,lastParentDataset),
					new Bubble(new DistributionMongoDBObject(distributionURI), true,lastParentDataset), diagram, 0);

			// get indegree and outdegree for a distribution
			ArrayList<LinksetMongoDBObject> in = LinksetQueries
					.getLinksetsInDegreeByDistribution(distributionURI);
			ArrayList<LinksetMongoDBObject> out = LinksetQueries
					.getLinksetsOutDegreeByDistribution(distributionURI);
			

			for (LinksetMongoDBObject linkset : in) {
				DistributionMongoDBObject a = new DistributionMongoDBObject(linkset.getDistributionSource());
				DistributionMongoDBObject b = new DistributionMongoDBObject(linkset.getDistributionTarget());
				
				if(a.getIsVocabulary() == false && b.getIsVocabulary() == false  )
				makeLink0(new Bubble(a, showDistribution,lastParentDataset),
						new Bubble(b, showDistribution,lastParentDataset), diagram, linkset.getLinks());

			}
			for (LinksetMongoDBObject linkset : out) {
				DistributionMongoDBObject a = new DistributionMongoDBObject(linkset.getDistributionSource());
				DistributionMongoDBObject b = new DistributionMongoDBObject(linkset.getDistributionTarget());
				
				if(!showOntologies){
					if(a.getIsVocabulary() == false && b.getIsVocabulary() == false  )				
						makeLink0(new Bubble(a, showDistribution,lastParentDataset),
								new Bubble(b, showDistribution,lastParentDataset), diagram, linkset.getLinks());
				}
				else
					makeLink0(new Bubble(a, showDistribution,lastParentDataset),
							new Bubble(b, showDistribution,lastParentDataset), diagram, linkset.getLinks());
				
			}

		}

	}

	private void makeLink0(Bubble source, Bubble target, Diagram diagram, int nLinks){
		
		Link link = new Link(source, target, nLinks);

		diagram.addBubble(target);
		diagram.addBubble(source);

		diagram.addLink(link);
	}
	
}
