package dynlod.mongodb.objects;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DataIDDB;

public class DistributionObjectDomainsMongoDBObject extends DataIDDB {

	// Collection name
	public static final String COLLECTION_NAME = "DistributionObjectDomains";

	
	// class properties
	public static final String DISTRIBUTION_URI = "distributionURI";
	
	public static final String OBJECT_FQDN = "objectFqdn";	
	
	
	private String distributionURI;

	private String objectFqdn;
	
	
	public DistributionObjectDomainsMongoDBObject(String uri) {
		
		super(COLLECTION_NAME, uri);
		loadObject();
	}

	public boolean updateObject(boolean checkBeforeInsert) throws DynamicLODGeneralException {

		BasicDBObject mongoDBObject2 = new BasicDBObject();
		
		// save object case it doens't exists
		try {
			// updating subjectsTarget on mongodb
			mongoDBObject.put(DISTRIBUTION_URI, distributionURI);
			mongoDBObject2.put(DISTRIBUTION_URI, distributionURI);

			// updating objectsTarget on mongodb
			mongoDBObject.put(OBJECT_FQDN, objectFqdn);
			mongoDBObject2.put(OBJECT_FQDN, objectFqdn);
			
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

			distributionURI = (String) obj.get(DISTRIBUTION_URI);

			objectFqdn = (String) obj.get(OBJECT_FQDN);

			return true;
		}
		return false;
	}
	
	public boolean remove(){
		BasicDBObject tmp = new BasicDBObject();
		tmp.put(DISTRIBUTION_URI, distributionURI);
		DBCursor d = objectCollection.find(tmp);
		objectCollection.remove(tmp);
		return true;
	}

	public String getDistributionURI() {
		return distributionURI;
	}

	public void setDistributionURI(String distributionURI) {
		this.distributionURI = distributionURI;
	}

	public String getObjectFQDN() {
		return objectFqdn;
	}

	public void setObjectFQDN(String objectFqdn) {
		this.objectFqdn = objectFqdn;
	}
	
	

}
