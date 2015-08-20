package dynlod.API.diagram;

import java.util.ArrayList;

import org.json.JSONObject;

import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.queries.DatasetQueries;

public class Bubble {

	Object dynLodObject;

	String uri;

	String text;

	String name;

	String color;
	
	boolean visible = false; 
	
	int radius;
	
	ArrayList<String> parentDataset = new ArrayList<String>();

	public JSONObject getJSON() {
		JSONObject node = new JSONObject();
		
		String group =  parentDataset.iterator().next();

		node.put("text", getText());
		node.put("group", parentDataset.iterator().next());
		DatasetMongoDBObject d = new DatasetMongoDBObject(group);
		
		if(!d.getTitle().equals(""))
			node.put("group_name", d.getTitle());
		else if(!d.getLabel().equals(""))
			node.put("group_name", d.getLabel());
		else
			node.put("group_name", group);		
		node.put("color", getColor());
		node.put("name", getUri());
		node.put("radius", getRadius());
		

		return node;
	}

	public Bubble(Object source, boolean visible) {
		this.visible = visible;
		startBubble(source);
	}
	
	public Bubble(Object source, boolean visible, String lastParentDataset) {
		this.visible = visible;
		parentDataset.add(lastParentDataset);
		startBubble(source);
	}
	
	public Bubble(Object source) {
		startBubble(source);
	}
	
	private void startBubble(Object source){
		if (source instanceof DistributionMongoDBObject) {

			DistributionMongoDBObject tmp = (DistributionMongoDBObject) source;
			if (tmp.getTitle() != null && !tmp.getTitle().equals(""))
				setText(tmp.getTitle());
			else
				setText(tmp.getUri());
			setName(tmp.getDownloadUrl());
			setUri(tmp.getUri());
	
				setRadius(31);


			if (tmp.getIsVocabulary()){
				setColor("rgb(253, 174, 107)");
				setRadius(27);
			}

			else
			setColor("rgb(66, 136, 78)");
		

			dynLodObject = (DistributionMongoDBObject) source;
		}

		else if (source instanceof DatasetMongoDBObject) {
			DatasetMongoDBObject tmp = (DatasetMongoDBObject) source;

			if (tmp.getTitle() != null || !tmp.getTitle().equals(""))
				setText(tmp.getTitle());
			else if (tmp.getLabel() != null || !tmp.getLabel().equals(""))
				setText(tmp.getLabel());
			else
				setText(tmp.getUri());
			
			setName(tmp.getUri());
			setUri(tmp.getUri());
		

			setRadius(31);

			if (tmp.getIsVocabulary()){
				setColor("rgb(253, 174, 107)");
				setRadius(27);
			}
			else
			setColor("rgb(116, 196, 118)");

			dynLodObject = (DatasetMongoDBObject) source;
		}
	}

	public String getText() {

		setText(text.split("@")[0]);
		setText(text.split("http")[0]);

		if (text.length() > 145) {
			setText(text.substring(0, 145) + "...");
		}
		return text;
//		return String.valueOf(isVisible());
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getColor() {
		return color;
	}

	public void setColor(String color) {
		this.color = color;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public int getRadius() {
		return radius;
	}

	public void setRadius(int radius) {
		this.radius = radius;
	}

	public ArrayList<String> getParentDataset() {
		return parentDataset;
	}

	public void addParentDataset(String parentDataset) {
		if(!this.parentDataset.contains(parentDataset)){
			this.parentDataset.add(parentDataset);
		}
	}

	public boolean isVisible() {
		return visible;
	}

	public void setVisible(boolean visible) {
		this.visible = visible;
	}
	
	
	

}
