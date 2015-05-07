package dataid.diagram;

import org.json.JSONObject;

import dataid.mongodb.objects.DatasetMongoDBObject;
import dataid.mongodb.objects.DistributionMongoDBObject;

public class Bubble {

	Object dynLodObject;

	String uri;

	String text;

	String name;

	String color;

	public JSONObject getJSON() {
		JSONObject node = new JSONObject();

		node.put("text", getText());
		node.put("color", getColor());
		node.put("name", getUri());
		node.put("radius", 20);

		return node;
	}

	public Bubble(Object source) {
		if (source instanceof DistributionMongoDBObject) {

			DistributionMongoDBObject tmp = (DistributionMongoDBObject) source;

			setText(tmp.getTitle());
			setName(tmp.getUri());
			setUri(tmp.getUri());

			if (tmp.getIsVocabulary())
				setColor("rgb(255, 127, 14)");
			else
				setColor("green");

			dynLodObject = (DistributionMongoDBObject) source;
		}

		else if (source instanceof DatasetMongoDBObject) {
			DatasetMongoDBObject tmp = (DatasetMongoDBObject) source;

			setText(tmp.getTitle());
			setName(tmp.getTitle());

			if (tmp.getIsVocabulary())
				setColor("rgb(255, 127, 14)");
			else
				setColor("green");

			dynLodObject = (DatasetMongoDBObject) source;
		}

	}

	public String getText() {
		if (text == null)
			setText("");
		setText(text.split("@")[0]);
		setText(text.split("http")[0]);

		if (text.length() > 1445) {
			setText(text.substring(0, 15) + "...");
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

}
