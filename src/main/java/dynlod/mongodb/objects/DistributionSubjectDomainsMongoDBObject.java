package dynlod.mongodb.objects;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dynlod.exceptions.DataIDException;
import dynlod.mongodb.DataIDDB;

public class DistributionSubjectDomainsMongoDBObject extends DataIDDB {
	// Collection name
		public static final String COLLECTION_NAME = "DistributionSubjectDomains";

		
		// class properties
		public static final String DISTRIBUTION_URI = "distributionURI";
		
		public static final String SUBJECT_DOMAIN = "subjectDomain";	
		
		
		private String distributionURI;

		private String subjectDomain;
		
		
		public DistributionSubjectDomainsMongoDBObject(String uri) {
			
			super(COLLECTION_NAME, uri);
			loadObject();
		}

		public boolean updateObject(boolean checkBeforeInsert) throws DataIDException {

			BasicDBObject mongoDBObject2 = new BasicDBObject();
			
			// save object case it doens't exists
			try {
				// updating subjectsTarget on mongodb
				mongoDBObject.put(DISTRIBUTION_URI, distributionURI);
				mongoDBObject2.put(DISTRIBUTION_URI, distributionURI);

				// updating objectsTarget on mongodb
				mongoDBObject.put(SUBJECT_DOMAIN, subjectDomain);
				mongoDBObject2.put(SUBJECT_DOMAIN, subjectDomain);
				
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

				distributionURI = (String) obj.get(DISTRIBUTION_URI);

				subjectDomain = (String) obj.get(SUBJECT_DOMAIN);

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

		public String getSubjectDomain() {
			return subjectDomain;
		}

		public void setSubjectDomain(String subjectDomain) {
			this.subjectDomain = subjectDomain;
		}

	
		
}
