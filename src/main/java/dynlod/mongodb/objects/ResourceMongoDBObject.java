package dynlod.mongodb.objects;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DBSuperClass;

public class ResourceMongoDBObject extends DBSuperClass {
	

	public static final String DYN_LOD_ID = "dynLodID";
	
	public static final String IS_VOCABULARY = "isVocabulary";
	
	public static final String TITLE = "title";
	
	public static final String LABEL = "label";
	
	
	
	protected String title;
	
	protected String label;
	
	protected boolean isVocabulary = false;
	
	protected int dynLodID = 0;
	
//	
//	@Override
//	protected DBObject searchByID(int id) {
//		try {
//			this.dynLodID = id;
//			mongoDBObject.put(DYN_LOD_ID,  id);
//
//			// adding object URI
////			if (uri == null)
////				return null;
//
//			DBCursor d = objectCollection.find(mongoDBObject);
//			if (d.hasNext()){
//				DBObject o = d.next();
//				this.uri = o.get(URI).toString();
//				return o;
//			}
//
//			throw new DynamicLODGeneralException("We coild not find dataset with id: "+ id);
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
//		
	protected DBObject search(int id) {

		mongoDBObject.put(DYN_LOD_ID,  id);
		DBCursor d = objectCollection.find(mongoDBObject);
		if (d.hasNext()){
			DBObject o = d.next();
			this.uri = o.get(URI).toString();
			return o;
		}

		return null;
	}
	

	public ResourceMongoDBObject(String collectionName, String uri) {
		super(collectionName, uri);
		// TODO Auto-generated constructor stub
	}
	
	public ResourceMongoDBObject(String collectionName, int id) {
		super(collectionName, id);
		// TODO Auto-generated constructor stub
	}
	
	public ResourceMongoDBObject(String collectionName, DBObject object) {
		super(collectionName, object);
		// TODO Auto-generated constructor stub
	}
	
	public ResourceMongoDBObject(String collection, String uri, boolean isRegex) {
		super(collection, uri, isRegex);
		loadObject();
	}

	@Override
	public boolean updateObject(boolean checkBeforeInsert)
			throws DynamicLODGeneralException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean loadObject() {
		return false;
	}

}