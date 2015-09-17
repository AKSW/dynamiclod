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

import dynlod.DynlodGeneralProperties;
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

	String LINK_TYPE;

	boolean showDistribution = true;
	
	boolean showOntologies = false;
	
	boolean showInvalidLinks = false;
	
	double min = 0;
	
	double max = 1;
	

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
		checkRange(parameters);
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
					.getLinksetsInDegreeByDistribution(distribution.getDynLodID(), LINK_TYPE,min, max);
			ArrayList<LinksetMongoDBObject> out = new LinksetQueries()
					.getLinksetsOutDegreeByDistribution(distribution.getDynLodID(), LINK_TYPE,min, max);
			

			for (LinksetMongoDBObject linkset : in) {
				DistributionMongoDBObject source =  new DistributionMongoDBObject(linkset.getDistributionSource());
				DistributionMongoDBObject target =  new DistributionMongoDBObject(linkset.getDistributionTarget());
				
				String links = getLinksCorrectFormat(linkset);
				
				if(!showOntologies){
				if(source.getIsVocabulary() == false && target.getIsVocabulary() == false)
					makeLink(diagram.addBubble(new Bubble(source, showDistribution,parentDataset)),
							diagram.addBubble(new Bubble(target, showDistribution,parentDataset)), diagram, links);
				}
				else
					makeLink(diagram.addBubble(new Bubble(source, showDistribution,parentDataset)),
							diagram.addBubble(new Bubble(target, showDistribution,parentDataset)), diagram, links);

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
		
		if(LINK_TYPE.equals(LinksetMongoDBObject.LINK_STRENGHT) || LINK_TYPE.equals(LinksetMongoDBObject.LINK_SIMILARITY)){
			if(!link.equals("S")){
				double linkV = Double.valueOf(link);
				if(linkV<=max && linkV>=min){
			
					Link l = new Link(source, target, link);

					diagram.addBubble(target);
					diagram.addBubble(source);
					
					diagram.addLink(l);
				}
			}
			else{
				Link l = new Link(source, target, link);

				diagram.addBubble(target);
				diagram.addBubble(source);
				
				diagram.addLink(l);
			}
		}
		else if(LINK_TYPE.equals(LinksetMongoDBObject.LINK_NUMBER_LINKS)){
			Link l = new Link(source, target, link);

			diagram.addBubble(target);
			diagram.addBubble(source);
			
			diagram.addLink(l);
		}
		
	}
	
	protected String getLinksCorrectFormat(LinksetMongoDBObject linkset){
		String links;
		if(showInvalidLinks)
			links = linkset.getInvalidLinksAsString();
		else if(LINK_TYPE.equals(LinksetMongoDBObject.LINK_SIMILARITY))
			links = linkset.getSimilarityAsString();
		else if(LINK_TYPE.equals(LinksetMongoDBObject.LINK_STRENGHT)){
			links = linkset.getStrengthAsString();
			
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
			min = Double.parseDouble(parameters.get("linkFrom")[0]);
		
		if (parameters.containsKey("linkTo")) 
			max = Double.parseDouble(parameters.get("linkTo")[0]);
	}
	
	protected void checkLinkTypes(Map<String, String[]> parameters){
		if(checkParamenter(parameters, "linkType")){
//			if(parameters.get("linkType")[0].equals("showInvalidLinks"))
//				showInvalidLinks = true;
			
			if(parameters.get("linkType")[0].equals("showLinksStrength"))
				LINK_TYPE = LinksetMongoDBObject.LINK_STRENGHT;
			else if(parameters.get("linkType")[0].equals("showSimilarity"))
				LINK_TYPE = LinksetMongoDBObject.LINK_SIMILARITY;
			else if(parameters.get("linkType")[0].equals("showLinks")){
				LINK_TYPE = LinksetMongoDBObject.LINK_NUMBER_LINKS;
				min = 50;
				max = -1;
			}
				
		}
	}
	
	
	
}
