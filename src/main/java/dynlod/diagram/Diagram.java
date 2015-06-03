package dynlod.diagram;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONArray;

public class Diagram {

	ArrayList<Link> links = new ArrayList<Link>();

	HashMap<String, Bubble> bubbles = new HashMap<String, Bubble>();

	public void addBubble(Bubble source) {
		if (!bubbles.containsKey(source.getUri())) {
			bubbles.put(source.getUri(), source);
		} else {
			Bubble b = bubbles.get(source.getUri());
			b.setText(source.getText());
			if(source.isVisible()) b.setVisible(true);
			if(!source.isVisible() && b.isVisible()) b.setVisible(true);
			else if(!source.isVisible()) b.setVisible(false);
			
			for (String parent : source.getParentDataset()) {
				b.addParentDataset(parent);
			}

		}
	}

	public void addLink(Link link) {
		if (link.source.name.equals(link.target.name))
			return;
		for (Link l : links) {
			if (l.source.name.equals(link.source.name)
					&& l.target.name.equals(link.target.name))
				return;
		}
		links.add(link);
	}

	public JSONArray getBubblesJSON() {
		JSONArray nodes = new JSONArray();

		for (Bubble b : bubbles.values()) {
			nodes.put(b.getJSON());

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

	public void printSelectedBubbles(String[] b1) {
		for (Bubble bubble : bubbles.values()) {
			for (String b : b1) {
				if (bubble.uri.equals(b)) {
					bubble.setColor("rgb(189, 189, 189)");
				}
			}
		}
	}

	public boolean checkIfBubbleExists(String bubbleURI) {
		if (bubbles.containsKey(bubbleURI))
			return true;
		else
			return false;
	}

	public ArrayList<Link> getLinks() {
		return links;
	}

	public void setLinks(ArrayList<Link> links) {
		this.links = links;
	}

	public ArrayList<Bubble> getBubbles() {
		return new ArrayList<Bubble>(bubbles.values());
	}

}
