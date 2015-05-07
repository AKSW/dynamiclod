package dataid.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import dataid.diagram.Bubble;
import dataid.diagram.Diagram;
import dataid.diagram.Link;
import dataid.mongodb.objects.DatasetMongoDBObject;
import dataid.mongodb.objects.DistributionMongoDBObject;
import dataid.mongodb.objects.LinksetMongoDBObject;
import dataid.mongodb.queries.LinksetQueries;

public class CreateD3JSONFormat extends HttpServlet {
	

	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	public void printOutput(JSONArray nodes, JSONArray links, HttpServletResponse response){
		
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
		
		if(parameters.containsKey("getAllDistributions")){
			
			Diagram diagram = new Diagram();
			ArrayList<LinksetMongoDBObject> linksets = LinksetQueries.getLinksetsWithLinks();
			
			for (LinksetMongoDBObject linkset : linksets) {
				
				Bubble source = new Bubble(new DistributionMongoDBObject(linkset.getObjectsDistributionTarget()));
				Bubble target = new Bubble(new DistributionMongoDBObject(linkset.getObjectsDistributionTarget()));
				
				Link link = new Link(source, target, linkset.getLinks());
				
				diagram.addBubble(target);
				diagram.addBubble(source);
				
				diagram.addLink(link);
			}
			
			nodes = diagram.getBubblesJSON();
			links = diagram.getLinksJSON();
		}
		
		printOutput(nodes, links, response);
//		
//		
//
//		String paramDataset = null;
//		boolean hasParameters = false;
//
//		try {
//			paramDataset = request.getParameter("dataset");
//			if (paramDataset != null){
//				if(!paramDataset.equals("")){
//				paramDataset = request.getParameter("dataset").replace(
//						"@@@@@@", "#");
//				hasParameters = true;
//				System.out.println(paramDataset);
//			}
//			}
//
//			JSONObject obj = new JSONObject();
//
//			ArrayList<LinksetMongoDBObject> linkList = null;
//			
//			System.out.println(hasParameters);
//
//			if (!hasParameters) {
//				linkList = LinksetQueries.getLinksetsGroupByDatasets();
//			} else {
//				linkList = LinksetQueries
//						.getLinksetsFilterByDataset(paramDataset);
//				
//			}
//
//			if (linkList != null)
//				for (LinksetMongoDBObject singleLink : linkList) {
//					
//					// if (!singleEdge.getObjectsDatasetTarget().equals(
//					// singleEdge.getSubjectsDatasetTarget())) {
//					JSONObject link = null;
//
//					JSONObject edgeDetail = new JSONObject();
//					// edgeDetail.put("directed", true);
//					edgeDetail.put("color", "red");
//					if (singleLink.getLinks() > 0) {
//
//						if(!hasParameters){
//							edgeDetail.put("target", singleLink
//									.getSubjectsDatasetTarget().toString());
//							edgeDetail.put("source", singleLink
//									.getObjectsDatasetTarget().toString());
//							edgeDetail.put("value", 5);
//							links.put(edgeDetail);
//							addNode(singleLink.getSubjectsDatasetTarget()
//									.toString(), hasParameters, nodeList, nodes);
//							addNode(singleLink.getObjectsDatasetTarget().toString(), hasParameters, nodeList, nodes);
//
//						}
//						else{
//							edgeDetail.put("target", singleLink
//									.getSubjectsDistributionTarget().toString());
//							edgeDetail.put("source", singleLink
//									.getObjectsDistributionTarget().toString());
//							
//							edgeDetail.put("value", 5);
//							links.put(edgeDetail);
//							addNode(singleLink.getSubjectsDistributionTarget()
//									.toString(), hasParameters, nodeList, nodes);
//							addNode(singleLink.getObjectsDistributionTarget().toString(), hasParameters, nodeList, nodes);
//
//						}
//					}
//				}
//
//			printOutput(nodes, links, response);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//
//	public void addNode(String link, boolean hasParameters, ArrayList<String> nodeList, JSONArray nodes) {
//		if (!nodeList.contains(link)) {
//			nodeList.add(link);
//			JSONObject node = new JSONObject();
//			
//			String text = "";
//			String name= "";
//			String color= "";
//
//			if (!hasParameters) {
//				DatasetMongoDBObject dt = new DatasetMongoDBObject(link);
//				
//				if(dt.getTitle()!=null)
//					text =dt.getTitle();
//				else
//					text= dt.getLabel();
//				if(dt.getIsVocabulary())
//					color = "rgb(255, 127, 14)";
//				else
//					color = "green";
//					
//				name= dt.getUri();
//			} else {
//				DistributionMongoDBObject dt = new DistributionMongoDBObject(
//						link);
//
//				if(dt.getTitle()!=null)
//					text = dt.getTitle();
//				else
//					text = dt.getDownloadUrl();
//				
//				if(dt.getIsVocabulary())
//					color = "rgb(255, 127, 14)";
//				else
//					color =  "green";
//				name= dt.getUri();
//				
//			}
//			
//			if(text==null)
//				text = "";
//			text = text.split("@")[0];
//			text = text.split("http")[0];
//			if(text.length()>1445){
//				text = text.substring(0, 15) + "...";
//			}
////			text = text.split(" ")[0] + "<br>"+ text.split(" ")[1];
////			if(text.split(" ").length > 1)
////				text = text.split(" ")[0] + "<br>"+ text.split(" ")[1];
//			node.put("text",text);
//			
//			node.put("color",color);
//			node.put("name",name);
//					
//			node.put("radius", 20);
//			nodes.put(node);
//		}
	}
}
