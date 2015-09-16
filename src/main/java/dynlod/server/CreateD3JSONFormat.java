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
import dynlod.mongodb.queries.FQDNQueries;
import dynlod.mongodb.queries.LinksetQueries;

public class CreateD3JSONFormat extends HttpServlet {

	private static final long serialVersionUID = -7213269624452749676L;

	boolean showDistribution = true;
	
	boolean showOntologies = false;
	
	boolean showInvalidLinks = false;
	
	boolean showLinks = false;
	
	boolean showLinksStrength = false;
	
	boolean showSimilarity = false;
	
	double linkFrom = 0;
	
	double linkTo = 1;
	

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
			ServletOutputStream out = response.getOutputStream();
			out.write(obj.toString().getBytes("UTF-8"));
//			response.getWriter().print(obj);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void manageRequest(HttpServletRequest request,
			HttpServletResponse response) {

		JSONArray nodes = new JSONArray();
		JSONArray links = new JSONArray();

		Map<String, String[]> parameters = request.getParameterMap();

		showDistribution = checkParamenter(parameters, "showDistributions");
		showOntologies = checkParamenter(parameters, "showOntologies");
		checkLinkTypes(parameters);
		
		
		if (parameters.containsKey("getAllDistributions")) {

			Diagram diagram = new Diagram();
			ArrayList<LinksetMongoDBObject> linksets = new LinksetQueries()
					.getLinksetsWithLinks();

			for (LinksetMongoDBObject linkset : linksets) {

				Bubble target = new Bubble( new DistributionMongoDBObject(
						linkset.getDistributionTarget()));
				Bubble source = new Bubble( new DistributionMongoDBObject(
						linkset.getDistributionSource()));

				Link link = new Link(source, target, linkset.getLinksAsString());

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
			for (String datasetID : parameters.get("dataset")) {

				int currentLevel = 1;
//				if (parameters.containsKey("level")) {
//					currentLevel = Integer.parseInt(parameters.get("level")[0]);
//				}
				
				DatasetMongoDBObject d = new DatasetMongoDBObject(Integer.valueOf(datasetID));

				iterateDataset(d, diagramTemp, d.getDynLodID(), currentLevel);				
			}
			
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

			

			int[] results = new int[parameters.get("dataset").length];

			for (int i = 0; i < results.length; i++) {
			    try {
			        results[i] = Integer.parseInt(parameters.get("dataset")[i]);
			    } catch (NumberFormatException nfe) {};
			}
			
			diagramTemp.printSelectedBubbles(results);
			
			nodes = diagramTemp.getBubblesJSON();
			links = diagramTemp.getLinksJSON();

			printOutput(nodes, links, response);
		}

	}

	private void iterateDataset(DatasetMongoDBObject dataset,
			Diagram diagram, int parentDataset, int currentLevel) {
		
		
		for (DatasetMongoDBObject subset : dataset.getSubsetsAsMongoDBObject()) {
			makeLink(diagram.addBubble(new Bubble(dataset, true,parentDataset)),
					diagram.addBubble(new Bubble(subset, true,parentDataset)), diagram, "S");				
			iterateDataset(subset, diagram, parentDataset, --currentLevel);
		}
		
		for (DistributionMongoDBObject distribution : dataset.getDistributionsAsMongoDBObjects()) {
			

			makeLink(diagram.addBubble(new Bubble(dataset, true,parentDataset)),
					diagram.addBubble(new Bubble(distribution, true,parentDataset)), diagram, "S");

			// get indegree and outdegree for a distribution
			ArrayList<LinksetMongoDBObject> in = new LinksetQueries()
					.getLinksetsInDegreeByDistribution(distribution.getDynLodID(), showInvalidLinks);
			ArrayList<LinksetMongoDBObject> out = new LinksetQueries()
					.getLinksetsOutDegreeByDistribution(distribution.getDynLodID(), showInvalidLinks);
			

			for (LinksetMongoDBObject linkset : in) {
				DistributionMongoDBObject source =  new DistributionMongoDBObject(linkset.getDistributionSource());
				DistributionMongoDBObject target =  new DistributionMongoDBObject(linkset.getDistributionTarget());
				
				String links = getLinksCorrectFormat(linkset);
				
				if(source.getIsVocabulary() == false && target.getIsVocabulary() == false)
					makeLink(diagram.addBubble(new Bubble(source, showDistribution,parentDataset)),
							diagram.addBubble(new Bubble(target, showDistribution,parentDataset)), diagram, links);
					
//				makeLink0(new Bubble(a, showDistribution,parentDataset),
//						new Bubble(b, showDistribution,parentDataset), diagram, linkset.getLinks());

			}
			for (LinksetMongoDBObject linkset : out) {
				DistributionMongoDBObject source =  new DistributionMongoDBObject(linkset.getDistributionSource());
				DistributionMongoDBObject target =  new DistributionMongoDBObject(linkset.getDistributionTarget());
				
				
				String links = getLinksCorrectFormat(linkset);
				
				if(!showOntologies){
					if(source.getIsVocabulary() == false && target.getIsVocabulary() == false  )				
						makeLink(diagram.addBubble(new Bubble(source, showDistribution,parentDataset)),
								diagram.addBubble(new Bubble(target, showDistribution,parentDataset)), diagram, links);
				}
				else
					makeLink(diagram.addBubble(new Bubble(source, showDistribution,parentDataset)),
							diagram.addBubble(new Bubble(target, showDistribution,parentDataset)), diagram, links);
				
			}
		}
	}
	

	private void makeLink(Bubble source, Bubble target, Diagram diagram, String link){
		
//		double nLinks = 0.0;
		
		// check if link refer to a subset or a number of links
//		if(!link.equals("S")){
//			nLinks = Double.parseDouble(link);
//			
//			
//		}
//		else{
//			
//		}
		
		// get number of described fqdn
//		int numberOfSourceFQDN = new FQDNQueries().getNumberOfObjectResources(source.getID());
		
//		if (numberOfSourceFQDN>0)
//			nLinks = 1.0*nLinks/numberOfSourceFQDN;
		

//		System.out.println(nLinks);
		
		// compare range
//		if(link<=linkTo && link>=linkFrom){
		
		Link l = new Link(source, target, link);

		diagram.addBubble(target);
		diagram.addBubble(source);
		
		diagram.addLink(l);
//		}
		
		
	}
	
	protected String getLinksCorrectFormat(LinksetMongoDBObject linkset){
		String links;
		if(showInvalidLinks)
			links = linkset.getInvalidLinksAsString();
		else if(showSimilarity)
			links = linkset.getJaccardSimilarityAsString();
		else if(showLinksStrength){
			double nLinks = 0.0;
			// get number of described fqdn
			int numberOfSourceFQDN = new FQDNQueries().getNumberOfObjectResources(linkset.getDistributionSource());
			
			if (numberOfSourceFQDN>0)
				nLinks = 1.0*linkset.getLinks()/numberOfSourceFQDN;
			links = String.valueOf(nLinks);
		}
		else
			links = linkset.getLinksAsString();
		
		return links;
	}
	
	protected boolean checkParamenter(Map<String, String[]> parameters, String parameter){
		if (parameters.containsKey(parameter)) 
			return true;
		else
			return false;
	}
	
	protected void checkRange(Map<String, String[]> parameters){
		if (parameters.containsKey("linkFrom")) 
			linkFrom = Double.parseDouble(parameters.get("linkFrom")[0]);
		
		if (parameters.containsKey("linkTo")) 
			linkTo = Double.parseDouble(parameters.get("linkTo")[0]);
	}
	
	protected void checkLinkTypes(Map<String, String[]> parameters){
		if(checkParamenter(parameters, "linkType")){
			if(parameters.get("linkType")[0].equals("showInvalidLinks"))
				showInvalidLinks = true;
			else if(parameters.get("linkType")[0].equals("showLinksStrength"))
				showLinksStrength = true;
			else if(parameters.get("linkType")[0].equals("showSimilarity"))
				showSimilarity = true;
				
		}
	}
	
	
	
}
