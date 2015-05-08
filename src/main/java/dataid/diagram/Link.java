package dataid.diagram;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;

import dataid.mongodb.objects.DatasetMongoDBObject;
import dataid.mongodb.objects.DistributionMongoDBObject;

public class Link {

	Bubble source;

	Bubble target;

	int links;

	public Link(Bubble source, Bubble target, int links) {
		this.source = source;
		this.target = target;
		this.links = links;
	}
	
	public JSONObject getJSON(){
		JSONObject link = new JSONObject();
		
		link.put("target", target
				.getUri());
		link.put("source", source
				.getUri());
		link.put("value", links);
		
		return link;
	}

}
