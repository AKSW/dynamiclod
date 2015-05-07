package dataid.diagram;

import java.util.ArrayList;

import org.json.JSONArray;

public class Diagram {

	ArrayList<String> listBubbleName = new ArrayList<String>();

	ArrayList<Link> links = new ArrayList<Link>();

	ArrayList<Bubble> bubbles = new ArrayList<Bubble>();

	public void addBubble(Bubble source) {
		if (!listBubbleName.contains(source.getUri())) {
			listBubbleName.add(source.getUri());
			bubbles.add(source);
		}
	}

	public void addLink(Link link) {
		links.add(link);
	}

	public JSONArray getBubblesJSON() {
		JSONArray nodes = new JSONArray();

		for (Bubble bubble : bubbles) {
			nodes.put(bubble.getJSON());
		}
		
		return nodes;
	}
	
	public JSONArray getLinksJSON() {
		JSONArray edges = new JSONArray();

		for (Link link : links) {
			edges.put(link.getJSON());
		}
		
		return edges;
	}

}