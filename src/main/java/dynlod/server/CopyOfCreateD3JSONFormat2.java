package dynlod.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import dynlod.API.diagram.Bubble;
import dynlod.API.diagram.Diagram;
import dynlod.API.diagram.Link;
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.queries.DatasetQueries;
import dynlod.mongodb.queries.DistributionQueries;
import dynlod.mongodb.queries.LinksetQueries;

public class CopyOfCreateD3JSONFormat2 extends HttpServlet {

//	
//	boolean showDistribution = true;
//	
//	boolean showOntologies = false;
//	
//
//	protected void doPost(HttpServletRequest request,
//			HttpServletResponse response) throws ServletException, IOException {
//		manageRequest(request, response);
//	}
//
//	protected void doGet(HttpServletRequest request,
//			HttpServletResponse response) throws ServletException, IOException {
//		manageRequest(request, response);
//	}
//
//	public void printOutput(JSONArray nodes, JSONArray links,
//			HttpServletResponse response) {
//
//		JSONObject obj = new JSONObject();
//
//		obj.put("nodes", nodes);
//		obj.put("links", links);
//
//		try {
//			ServletOutputStream out = response.getOutputStream();
//			out.write(obj.toString().getBytes("UTF-8"));
////			response.getWriter().print(obj);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//
//	public void manageRequest(HttpServletRequest request,
//			HttpServletResponse response) {
//
//		JSONArray nodes = new JSONArray();
//		JSONArray links = new JSONArray();
//
//		Map<String, String[]> parameters = request.getParameterMap();
//
//		if (parameters.containsKey("showDistributions")) 
//			showDistribution = true;
//		else
//			showDistribution = false;
//			
//		
//
//		if (parameters.containsKey("showOntologies")) 
//			showOntologies = true;
//		else
//			showOntologies = false;
//		
//		
//		if (parameters.containsKey("getAllDistributions")) {
//
//			Diagram diagram = new Diagram();
//			ArrayList<LinksetMongoDBObject> linksets = new LinksetQueries()
//					.getLinksetsWithLinks();
//
//			for (LinksetMongoDBObject linkset : linksets) {
//
//				Bubble target = new Bubble( new DistributionMongoDBObject(
//						linkset.getDistributionTarget()));
//				Bubble source = new Bubble( new DistributionMongoDBObject(
//						linkset.getDistributionSource()));
//
//				Link link = new Link(source, target, linkset.getLinks());
//
//				diagram.addBubble(target);
//				diagram.addBubble(source);
//
//				diagram.addLink(link);
//			}
//
//			nodes = diagram.getBubblesJSON();
//			links = diagram.getLinksJSON();
//			printOutput(nodes, links, response);
//		}
//
//		if (parameters.containsKey("dataset")) {
//			Diagram diagramTemp = new Diagram();
//			for (String datasetID : parameters.get("dataset")) {
//
//				int currentLevel = 1;
////				if (parameters.containsKey("level")) {
////					currentLevel = Integer.parseInt(parameters.get("level")[0]);
////				}
//				
//				DatasetMongoDBObject d = new DatasetMongoDBObject(Integer.valueOf(datasetID));
//
//				iterateDataset(d, diagramTemp, d.getUri(), currentLevel);				
//			}
//			
//			Diagram diagram = new Diagram();
//			
////			for (Bubble bubble : diagramTemp.getBubbles()) {
////				if(bubble.isVisible())
////					diagram.addBubble(bubble);
////			}
//			
////			for (Link link : diagramTemp.getLinks()) {
////				
////				if(link.getSource().isVisible() && link.getTarget().isVisible()){
////					makeLink0(link.getSource(), link.getTarget(), diagram, diagramTemp.getPathWeight(link.getSource().getName(), link.getTarget().getName())); 
////				}
////				else
////				for(String s: link.getSource().getParentDataset()){
////					for(String t: link.getTarget().getParentDataset()){			
////					makeLink0(new Bubble(new DatasetMongoDBObject(s)), 
////							new Bubble(new DatasetMongoDBObject(t)), diagram, 
////							diagramTemp.getPathWeight(s, t)); 
////					System.out.println(diagramTemp.getPathWeight(s, t));
////					}
////				}
////				
////			}
//
//			
//
//			int[] results = new int[parameters.get("dataset").length];
//
//			for (int i = 0; i < results.length; i++) {
//			    try {
//			        results[i] = Integer.parseInt(parameters.get("dataset")[i]);
//			    } catch (NumberFormatException nfe) {};
//			}
//			
//			diagramTemp.printSelectedBubbles(results);
//			
//			nodes = diagramTemp.getBubblesJSON();
//			links = diagramTemp.getLinksJSON();
//
//			printOutput(nodes, links, response);
//		}
//
//	}
//
//	private void iterateDataset(DatasetMongoDBObject d,
//			Diagram diagram, String lastParentDataset, int currentLevel) {
//		
//		
//		for (DatasetMongoDBObject subset : d.getSubsetsAsMongoDBObject()) {
//			makeLink0(new Bubble(d, true,lastParentDataset),
//					  new Bubble(subset, true,lastParentDataset), diagram, 0);				
//			iterateDataset(subset, diagram, lastParentDataset, --currentLevel);
//		}
//		
//		for (DistributionMongoDBObject distribution : d.getDistributionsAsMongoDBObjects()) {
//			
//
//			makeLink0(new Bubble(d, true,lastParentDataset),
//					new Bubble(distribution, true,lastParentDataset), diagram, 0);
//
//			// get indegree and outdegree for a distribution
//			ArrayList<LinksetMongoDBObject> in = new LinksetQueries()
//					.getLinksetsInDegreeByDistribution(distribution.getDynLodID());
//			ArrayList<LinksetMongoDBObject> out = new LinksetQueries()
//					.getLinksetsOutDegreeByDistribution(distribution.getDynLodID());
//			
//
//			for (LinksetMongoDBObject linkset : in) {
//				DistributionMongoDBObject a =  new DistributionMongoDBObject(linkset.getDistributionSource());
//				DistributionMongoDBObject b =  new DistributionMongoDBObject(linkset.getDistributionTarget());
//				
//				if(a.getIsVocabulary() == false && b.getIsVocabulary() == false  )
//				makeLink0(new Bubble(a, showDistribution,lastParentDataset),
//						new Bubble(b, showDistribution,lastParentDataset), diagram, linkset.getLinks());
//
//			}
//			for (LinksetMongoDBObject linkset : out) {
//				DistributionMongoDBObject a =  new DistributionMongoDBObject(linkset.getDistributionSource());
//				DistributionMongoDBObject b =  new DistributionMongoDBObject(linkset.getDistributionTarget());
//				
//				if(!showOntologies){
//					if(a.getIsVocabulary() == false && b.getIsVocabulary() == false  )				
//						makeLink0(new Bubble(a, showDistribution,lastParentDataset),
//								new Bubble(b, showDistribution,lastParentDataset), diagram, linkset.getLinks());
//				}
//				else
//					makeLink0(new Bubble(a, showDistribution,lastParentDataset),
//							new Bubble(b, showDistribution,lastParentDataset), diagram, linkset.getLinks());
//				
//			}
//
//		}
//
//	}
//
//	private void makeLink0(Bubble source, Bubble target, Diagram diagram, int nLinks){
//		
//		Link link = new Link(source, target, nLinks);
//
//		diagram.addBubble(target);
//		diagram.addBubble(source);
//
//		diagram.addLink(link);
//	}
	
}
