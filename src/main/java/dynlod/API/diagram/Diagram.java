package dynlod.API.diagram;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

import org.jgrapht.DirectedGraph;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.KShortestPaths;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.DirectedWeightedMultigraph;
import org.json.JSONArray;

public class Diagram {

	
	public DirectedWeightedMultigraph<String, DefaultWeightedEdge> g = new DirectedWeightedMultigraph<String, DefaultWeightedEdge>(
			DefaultWeightedEdge.class);

	
	ArrayList<Link> links = new ArrayList<Link>();

	public HashMap<String, Bubble> bubbles = new HashMap<String, Bubble>();

	public void addBubble(Bubble source) {
		if (!bubbles.containsKey(source.getUri())) {
			bubbles.put(source.getUri(), source);
			g.addVertex(source.getUri());
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
		DefaultWeightedEdge e = g.addEdge(link.source.name, link.target.name);
		g.setEdgeWeight(e,link.getLinks());
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
	
	/**
	 * All paths that lead from to to without crossing a Tap/Group boundary
	 * 
	 * @param graph
	 * @param from
	 * @param to
	 * @return
	 */
	public ArrayList<GraphPath<String, DefaultWeightedEdge>> getAllDirectPathsBetween(
			DirectedGraph<String, DefaultWeightedEdge> graph, String from,
			String to) {
		
		ArrayList<GraphPath<String, DefaultWeightedEdge>> paths = getAllShortestPathsBetween(
				graph, from, to);
		ArrayList<GraphPath<String, DefaultWeightedEdge>> results = new ArrayList<GraphPath<String, DefaultWeightedEdge>>(
				paths);
		for (GraphPath<String, DefaultWeightedEdge> path : paths) {
			ArrayList<String> pathVertexList = (ArrayList<String>) Graphs.getPathVertexList(path);
			for (int i = 1; i < pathVertexList.size(); i++) {
				String flowElement = (String) pathVertexList.get(i);
//				if (flowElement instanceof Tap || flowElement instanceof Group) {
//					results.remove(path);
//					break;
//				}
			}
		}
		return results;
	}
	
	public int getPathWeight(String source, String target){
		if(source.equals(target)) return 0;
		int weight = 0;
		for (GraphPath<String, DefaultWeightedEdge> path : getAllDirectPathsBetween(g, source, target)) {

			weight = (int) (weight + path.getWeight());
		}
		return weight;
	}

	public ArrayList<GraphPath<String, DefaultWeightedEdge>> getAllShortestPathsBetween(
			DirectedGraph<String, DefaultWeightedEdge> graph, String from,
			String to) {
		ArrayList<GraphPath<String, DefaultWeightedEdge>> paths = (ArrayList<GraphPath<String, DefaultWeightedEdge>>) new KShortestPaths<String, DefaultWeightedEdge>(
				graph, from, Integer.MAX_VALUE).getPaths(to);

		if (paths == null)
			return new ArrayList<GraphPath<String, DefaultWeightedEdge>>();

		return paths;
	}

}
