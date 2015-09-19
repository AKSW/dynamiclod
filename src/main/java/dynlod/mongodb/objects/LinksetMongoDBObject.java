package dynlod.mongodb.objects;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;

public class LinksetMongoDBObject extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "Linkset";

	// class properties
	public static final String DISTRIBUTION_TARGET = "distributionTarget";

	public static final String DISTRIBUTION_SOURCE = "distributionSource";

	public static final String DATASET_TARGET = "datasetTarget";

	public static final String DATASET_SOURCE = "datasetSource";

//	public static final String INVALID_LINKS = "invalidLinks";

	public static final String LINK_SIMILARITY = "similarity";

	public static final String LINK_STRENGHT = "strength";

	public static final String LINK_NUMBER_LINKS = "links";

	private int distributionTarget;

	private int distributionSource;

	private int datasetTarget;

	private int datasetSource;

	private double similarity;

	private double strength;

	private int links = 0;

	private int invalidLinks = 0;

	public LinksetMongoDBObject(String uri) {
		super(COLLECTION_NAME, uri);
		loadObject();
	}

	public LinksetMongoDBObject(DBObject object) {
		super(COLLECTION_NAME, object);
		load(object);
	}
	
	
	
	
	protected void load(DBObject obj){

		if (obj != null) {

			distributionTarget = ((Number) obj
					.get(DISTRIBUTION_TARGET)).intValue();

			distributionSource =  ((Number) obj
					.get(DISTRIBUTION_SOURCE)).intValue();

			datasetSource =  ((Number) obj.get(DATASET_SOURCE)).intValue();

			datasetTarget =  ((Number) obj.get(DATASET_TARGET)).intValue();

			similarity =  ((Number) obj.get(LINK_SIMILARITY)).doubleValue();

			strength =  ((Number) obj.get(LINK_STRENGHT)).doubleValue();

//			invalidLinks = Integer.valueOf(obj.get(INVALID_LINKS).toString());

			links = ((Number) obj.get(LINK_NUMBER_LINKS)).intValue();

		}
	}

	
	
	
	public boolean updateObject(boolean checkBeforeInsert) {

		// save object case it doens't exists
		try {
			// updating subjectsTarget on mongodb
			mongoDBObject.put(DISTRIBUTION_TARGET,
					distributionTarget);

			// updating objectsTarget on mongodb
			mongoDBObject.put(DISTRIBUTION_SOURCE,
					distributionSource);

			// updating subjectsTarget on mongodb
			mongoDBObject.put(DATASET_TARGET, datasetTarget);

			// updating objectsTarget on mongodb
			mongoDBObject.put(DATASET_SOURCE, datasetSource);

			// updating links on mongodb
			mongoDBObject.put(LINK_NUMBER_LINKS, links);

			// updating invalid links on mongodb
//			mongoDBObject.put(INVALID_LINKS, invalidLinks);
			

			// updating similarity on mongodb
			mongoDBObject.put(LINK_SIMILARITY, similarity);

			// updating link strength on mongodb
			mongoDBObject.put(LINK_STRENGHT, strength);

			insert(checkBeforeInsert);
		} catch (Exception e2) {
			// e2.printStackTrace();

			try {
				if (update())
					return true;
				else
					return false;
			} catch (DynamicLODGeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		}
		return false;
	}

	protected boolean loadObject() {
		DBObject obj = search();

		if (obj != null) {

			distributionTarget = ((Number) obj
					.get(DISTRIBUTION_TARGET)).intValue();

			distributionSource =  ((Number) obj
					.get(DISTRIBUTION_SOURCE)).intValue();

			datasetSource =  ((Number) obj.get(DATASET_SOURCE)).intValue();

			datasetTarget =  ((Number) obj.get(DATASET_TARGET)).intValue();

			similarity =  ((Number) obj.get(LINK_SIMILARITY)).doubleValue();

			strength =  ((Number) obj.get(LINK_STRENGHT)).doubleValue();

//			invalidLinks = Integer.valueOf(obj.get(INVALID_LINKS).toString());

			links = ((Number) obj.get(LINK_NUMBER_LINKS)).intValue();

			return true;
		}
		return false;
	}

	public int getDistributionTarget() {
		return distributionTarget;
	}

	public void setDistributionTarget(int subjectsDistributionTarget) {
		this.distributionTarget = subjectsDistributionTarget;
	}

	public int getDistributionSource() {
		return distributionSource;
	}

	public void setDistributionSource(int objectsDistributionTarget) {
		this.distributionSource = objectsDistributionTarget;
	}

	public int getDatasetTarget() {
		return datasetTarget;
	}

	public void setDatasetTarget(int subjectsDatasetTarget) {
		this.datasetTarget = subjectsDatasetTarget;
	}

	public int getDatasetSource() {
		return datasetSource;
	}

	public void setDatasetSource(int objectsDatasetTarget) {
		this.datasetSource = objectsDatasetTarget;
	}

	public int getLinks() {
		return links;
	}
	
	public String getLinksAsString(){
		return String.valueOf(links);
	}

	public void setLinks(int links) {
		this.links = links;
	}

	public int getInvalidLinks() {
		return invalidLinks;
	}
	
	public String getInvalidLinksAsString() {
		return String.valueOf(invalidLinks);
	}

	public void setInvalidLinks(int invalidLinks) {
		this.invalidLinks = invalidLinks;
	}

	public double getJaccardSimilarity() {
		return similarity;
	}
	
	public String getSimilarityAsString() {
		return String.valueOf(similarity);
	}

	public void setSimilarity(double similarity) {
		this.similarity = similarity;
	}

	public double getStrength() {
		return strength;
	}

	public String getStrengthAsString() {
		return String.valueOf(strength);
	}

	public void setStrength(double strength) {
		this.strength = strength;
	}

	
	
	
}
