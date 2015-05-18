package dynlod.diagram;

import org.json.JSONObject;

import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;

public class Bubble {

	Object dynLodObject;

	String uri;

	String text;

	String name;

	String color;
	
	int radius;

	public JSONObject getJSON() {
		JSONObject node = new JSONObject();

		node.put("text", getText());
		node.put("color", getColor());
		node.put("name", getUri());
		node.put("radius", getRadius());

		return node;
	}

	public Bubble(Object source) {
		if (source instanceof DistributionMongoDBObject) {

			DistributionMongoDBObject tmp = (DistributionMongoDBObject) source;
			if (tmp.getTitle() != null && !tmp.getTitle().equals(""))
				setText(tmp.getTitle());
			else
				setText(tmp.getUri());
			setName(tmp.getDownloadUrl());
			setUri(tmp.getUri());
			

//			if(tmp.getTriples()>10000000)
//				setRadius(28);
//			else
//			else{
//				setRadius(tmp.getTriples()/833333);
//			}
//			if(getRadius()<27)
				setRadius(31);

			if (tmp.getIsVocabulary()){
				setColor("rgb(255, 127, 14)");
				setRadius(27);
			}
			else
				setColor("green");

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
			setRadius(20);

			if (tmp.getIsVocabulary())
				setColor("rgb(255, 127, 14)");
			else
				setColor("green");

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
	
	

}
