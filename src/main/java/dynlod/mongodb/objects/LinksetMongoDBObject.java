package dynlod.mongodb.objects;

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

	public static final String AVAILABILITY = "availability";

	public static final String LINKS = "links";

	private String distributionTarget;

	private String distributionSource;

	private String datasetTarget;

	private String datasetSource;

	private int availability = 0;

	private int links = 0;

	public LinksetMongoDBObject(String uri) {
		super(COLLECTION_NAME, uri);
		loadObject();
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
			mongoDBObject.put(LINKS, links);

			// updating links on mongodb
			mongoDBObject.put(AVAILABILITY, availability);

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

			distributionTarget = (String) obj
					.get(DISTRIBUTION_TARGET);

			distributionSource = (String) obj
					.get(DISTRIBUTION_SOURCE);

			datasetSource = (String) obj.get(DATASET_SOURCE);

			datasetTarget = (String) obj.get(DATASET_TARGET);

			availability = Integer.valueOf(obj.get(AVAILABILITY).toString());

			links = Integer.valueOf(obj.get(LINKS).toString());

			return true;
		}
		return false;
	}

	public String getDistributionTarget() {
		return distributionTarget;
	}

	public void setDistributionTarget(String subjectsDistributionTarget) {
		this.distributionTarget = subjectsDistributionTarget;
	}

	public String getDistributionSource() {
		return distributionSource;
	}

	public void setDistributionSource(String objectsDistributionTarget) {
		this.distributionSource = objectsDistributionTarget;
	}

	public String getDatasetTarget() {
		return datasetTarget;
	}

	public void setDatasetTarget(String subjectsDatasetTarget) {
		this.datasetTarget = subjectsDatasetTarget;
	}

	public String getDatasetSource() {
		return datasetSource;
	}

	public void setDatasetSource(String objectsDatasetTarget) {
		this.datasetSource = objectsDatasetTarget;
	}

	public int getLinks() {
		return links;
	}

	public void setLinks(int links) {
		this.links = links;
	}

	public int getAvailability() {
		return availability;
	}

	public void setAvailability(int availability) {
		this.availability = availability;
	}

	
	
	
}
