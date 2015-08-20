package dynlod.API.diagram;

import org.json.JSONObject;

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

	public int getLinks() {
		return links;
	}

	public void setLinks(int links) {
		this.links = links;
	}

	

}
