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
				
				Bubble target = new Bubble(new DistributionMongoDBObject(linkset.getSubjectsDistributionTarget()));
				Bubble source = new Bubble(new DistributionMongoDBObject(linkset.getObjectsDistributionTarget()));
				
				Link link = new Link(source, target, linkset.getLinks());
				
				diagram.addBubble(target);
				diagram.addBubble(source);
				
				diagram.addLink(link);
			}
			
			nodes = diagram.getBubblesJSON();
			links = diagram.getLinksJSON();
			printOutput(nodes, links, response);
		}
	}
}
