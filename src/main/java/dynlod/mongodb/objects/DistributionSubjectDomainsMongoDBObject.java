package dynlod.mongodb.objects;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;

public class DistributionSubjectDomainsMongoDBObject extends DBSuperClass {
	// Collection name
		public static final String COLLECTION_NAME = "DistributionSubjectDomains";

		
		// class properties
		public static final String DISTRIBUTION_ID = "distributionID";
		
		public static final String SUBJECT_FQDN = "subjectFQDN";	
		
		
		private int distributionID;

		private String subjectFqdn;
		
		
		public DistributionSubjectDomainsMongoDBObject(String uri) {
			
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
				mongoDBObject.put(SUBJECT_FQDN, subjectFqdn);
				mongoDBObject2.put(SUBJECT_FQDN, subjectFqdn);
				
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

				subjectFqdn = (String) obj.get(SUBJECT_FQDN);

				return true;
			}
			return false;
		}
		
		public boolean remove(){
			BasicDBObject tmp = new BasicDBObject();
			tmp.put(DISTRIBUTION_ID, distributionID);
			DBCursor d = objectCollection.find(tmp);
			objectCollection.remove(tmp);
			return true;
		}

		public int getDistributionID() {
			return distributionID;
		}

		public void setDistributionID(int distributionID) {
			this.distributionID = distributionID;
		}

		public String getSubjectFQDN() {
			return subjectFqdn;
		}

		public void setSubjectFQDN(String subjectFQDN) {
			this.subjectFqdn = subjectFQDN;
		}

	
		
}
