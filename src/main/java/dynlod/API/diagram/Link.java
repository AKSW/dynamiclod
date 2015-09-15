package dynlod.API.diagram;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.json.JSONObject;

public class Link {

	Bubble source;

	Bubble target;

	double links;
	

	public Link(Bubble source, Bubble target, double links) {
		this.source = source;
		this.target = target;
		this.links = links;
	}
	
	public JSONObject getJSON(){
		NumberFormat formatter = new DecimalFormat("0.00000000");     
	
		JSONObject link = new JSONObject();
		
		link.put("target", target
				.getID());
		link.put("source", source
				.getID());
		link.put("value", formatter.format(links));
		
		return link;
	}

	public Bubble getSource() {
		return source;
	}

	public void setSource(Bubble source) {
		this.source = source;
	}

	public Bubble getTarget() {
		return target;
	}

	public void setTarget(Bubble target) {
		this.target = target;
	}

	public double getLinks() {
		return links;
	}

	public void setLinks(int links) {
		this.links = links;
	}

	

}
