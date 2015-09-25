package dynlod.mongodb.collections;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;

public class DistributionObjectDomainsMongoDBObject extends DBSuperClass {

	// Collection name
	public static final String COLLECTION_NAME = "DistributionObjectDomains";

	
	// class properties
	public static final String DISTRIBUTION_ID = "distributionID";
	
	public static final String OBJECT_FQDN = "objectFqdn";	
	
	public static final String NUMBER_OF_RESOURCES = "numberOfResources";	
	
	
	private int distributionID;

	private String objectFqdn;

	private int numberOfResources;

	
	
	public DistributionObjectDomainsMongoDBObject(String uri) {
		
		super(COLLECTION_NAME, uri);
		loadObject();
	}

	public boolean updateObject(boolean checkBeforeInsert) throws DynamicLODGeneralException {

		BasicDBObject mongoDBObject2 = new BasicDBObject();
		
		// save object case it doens't exists
		try {
			// updating subjectsTarget on mongodb
			mongoDBObject.put(DISTRIBUTION_ID, distributionID);
			mongoDBObject2.put(DISTRIBUTION_ID, distributionID);

			// updating objectsTarget on mongodb
			mongoDBObject.put(OBJECT_FQDN, objectFqdn);
			mongoDBObject2.put(OBJECT_FQDN, objectFqdn);
			
			// updating number of resources on mongodb
			mongoDBObject.put(NUMBER_OF_RESOURCES, numberOfResources);
			mongoDBObject2.put(NUMBER_OF_RESOURCES, numberOfResources);

			
			DBCursor d = objectCollection.find(mongoDBObject2);
			if (d.hasNext())
				return false;

			
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

			distributionID = ((Number) obj.get(DISTRIBUTION_ID)).intValue();

			objectFqdn = (String) obj.get(OBJECT_FQDN);

			numberOfResources = ((Number) obj.get(NUMBER_OF_RESOURCES)).intValue();


			return true;
		}
		return false;
	}

	public int getDistributionID() {
		return distributionID;
	}

	public void setDistributionID(int distributionID) {
		this.distributionID = distributionID;
	}
 
	public String getObjectFQDN() {
		return objectFqdn;
	}

	public void setObjectFQDN(String objectFqdn) {
		this.objectFqdn = objectFqdn;
	}

	public int getNumberOfResources() {
		return numberOfResources;
	}

	public void setNumberOfResources(int numberOfResources) {
		this.numberOfResources = numberOfResources;
	}
	
	

}