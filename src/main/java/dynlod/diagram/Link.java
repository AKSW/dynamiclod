package dynlod.diagram;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;

import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;

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
