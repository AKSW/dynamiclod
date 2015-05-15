package dynlod.mongodb.objects;

import com.mongodb.DBObject;

import dynlod.exceptions.DataIDException;
import dynlod.mongodb.DataIDDB;

public class LinksetMongoDBObject extends DataIDDB {

	// Collection name
	public static final String COLLECTION_NAME = "Linkset";

	// class properties
	public static final String SUBJECTS_DISTRIBUTION_TARGET = "subjectsDistributionTarget";

	public static final String OBJECTS_DISTRIBUTION_TARGET = "objectsDistributionTarget";

	public static final String SUBJECTS_DATASET_TARGET = "subjectsDatasetTarget";

	public static final String OBJECTS_DATASET_TARGET = "objectsDatasetTarget";

	public static final String AVAILABILITY = "availability";

	public static final String LINKS = "links";

	private String subjectsDistributionTarget;

	private String objectsDistributionTarget;

	private String subjectsDatasetTarget;

	private String objectsDatasetTarget;

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
			mongoDBObject.put(SUBJECTS_DISTRIBUTION_TARGET,
					subjectsDistributionTarget);

			// updating objectsTarget on mongodb
			mongoDBObject.put(OBJECTS_DISTRIBUTION_TARGET,
					objectsDistributionTarget);

			// updating subjectsTarget on mongodb
			mongoDBObject.put(SUBJECTS_DATASET_TARGET, subjectsDatasetTarget);

			// updating objectsTarget on mongodb
			mongoDBObject.put(OBJECTS_DATASET_TARGET, objectsDatasetTarget);

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
			} catch (DataIDException e) {
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

			subjectsDistributionTarget = (String) obj
					.get(SUBJECTS_DISTRIBUTION_TARGET);

			objectsDistributionTarget = (String) obj
					.get(OBJECTS_DISTRIBUTION_TARGET);

			objectsDatasetTarget = (String) obj.get(OBJECTS_DATASET_TARGET);

			subjectsDatasetTarget = (String) obj.get(SUBJECTS_DATASET_TARGET);

			availability = Integer.valueOf(obj.get(AVAILABILITY).toString());

			links = Integer.valueOf(obj.get(LINKS).toString());

			return true;
		}
		return false;
	}

	public String getSubjectsDistributionTarget() {
		return subjectsDistributionTarget;
	}

	public void setSubjectsDistributionTarget(String subjectsDistributionTarget) {
		this.subjectsDistributionTarget = subjectsDistributionTarget;
	}

	public String getObjectsDistributionTarget() {
		return objectsDistributionTarget;
	}

	public void setObjectsDistributionTarget(String objectsDistributionTarget) {
		this.objectsDistributionTarget = objectsDistributionTarget;
	}

	public String getSubjectsDatasetTarget() {
		return subjectsDatasetTarget;
	}

	public void setSubjectsDatasetTarget(String subjectsDatasetTarget) {
		this.subjectsDatasetTarget = subjectsDatasetTarget;
	}

	public String getObjectsDatasetTarget() {
		return objectsDatasetTarget;
	}

	public void setObjectsDatasetTarget(String objectsDatasetTarget) {
		this.objectsDatasetTarget = objectsDatasetTarget;
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
