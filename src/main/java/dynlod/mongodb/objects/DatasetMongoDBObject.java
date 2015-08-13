package dynlod.mongodb.objects;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.DataIDDB;

public class DatasetMongoDBObject extends DataIDDB {
	 
	// Collection name
	public static final String COLLECTION_NAME = "Dataset";

	public static final String LABEL = "label";

	public static final String TITLE = "title";
	
	public static final String PARENT_DATASETS = "parentDatasets";

	public static final String SUBSET_URIS = "subsetUris";

	public static final String DISTRIBUTIONS_URIS = "distributionsUris";

	public static final String DESCRIPTION_FILENAME = "descriptionFileName";
	
	public static final String OBJECT_FILENAME = "objectFileName";
	
	public static final String SUBJECT_FILTER_FILENAME = "subjectFileName";
	
	public static final String IS_VOCABULARY = "isVocabulary";
	
	public static final String ACCESS_URL = "accessUrl";
	
	public static final String DYN_LOD_ID = "dynLodID";
	
	

	// class properties

	private String label;

	private String title;

	private String descriptionFileName;

	private String access_url;

	private int dynLodID = 0;

	private boolean isVocabulary = false;
	

	public ArrayList<String> subsetsURIs = new ArrayList<String> ();

	public ArrayList<String>  distributionsURIs = new ArrayList<String> ();
	
	private ArrayList<String> parentDatasetsURI = new ArrayList<String>();

	public DatasetMongoDBObject(String uri) {
		super(COLLECTION_NAME, uri);
		loadObject();
	}
	public DatasetMongoDBObject(String uri, boolean isRegex) {
		super(COLLECTION_NAME, uri, isRegex);
		loadObject();
	}

	public boolean updateObject(boolean checkBeforeInsert) {

		// save object case it doens't exists
		try {
			// updating subsets on mongodb
			mongoDBObject.put(SUBSET_URIS, subsetsURIs);
			
			if(dynLodID == 0)
				dynLodID = new DynamicLODCounterMongoDBObject().incrementAndGetID();
			mongoDBObject.put(DYN_LOD_ID, dynLodID);
			
			// updating distributions on mongodb
			mongoDBObject.put(DISTRIBUTIONS_URIS, distributionsURIs);
	
			mongoDBObject.put(TITLE, title);

			mongoDBObject.put(DESCRIPTION_FILENAME, descriptionFileName);

			mongoDBObject.put(LABEL, label);
			
			mongoDBObject.put(IS_VOCABULARY, isVocabulary);
			
			mongoDBObject.put(PARENT_DATASETS, parentDatasetsURI);
			
			mongoDBObject.put(ACCESS_URL, access_url);
			
			
			insert(checkBeforeInsert);
			return true;
		} catch (Exception e2) {
//			e2.printStackTrace();

			try {
				if (update())
					return true;
				else
					return false;
			} catch (DynamicLODGeneralException e) {
				e.printStackTrace();
				return false;
			}
		}
	}

	protected boolean loadObject() {
		DBObject obj = search();

		if (obj != null) {
			// mongoDBObject = (BasicDBObject) obj;

			label = (String) obj.get(LABEL);
			uri = (String) obj.get(URI);
			title = (String) obj.get(TITLE);
			descriptionFileName = (String) obj.get(DESCRIPTION_FILENAME);
			isVocabulary = (Boolean) obj.get(IS_VOCABULARY);
			access_url = (String) obj.get(ACCESS_URL);
			dynLodID = (Integer) obj.get(DYN_LOD_ID);
			if(dynLodID == 0)
				dynLodID = new DynamicLODCounterMongoDBObject().incrementAndGetID();

			// loading subsets to object
			BasicDBList subsetList = (BasicDBList) obj.get(SUBSET_URIS);
			for (Object sd : subsetList) {
				subsetsURIs.add((String)sd);
			}

			// loading distributions to object
			BasicDBList distributionList = (BasicDBList) obj
					.get(DISTRIBUTIONS_URIS);
			for (Object sd : distributionList) {
				distributionsURIs.add((String)sd);
			}
			
			// loading parent datasets to object
			BasicDBList parentDatasetsList = (BasicDBList) obj
					.get(PARENT_DATASETS);
			for (Object sd : parentDatasetsList) {
				parentDatasetsURI.add((String) sd);
			}

			return true;
		}
		return false;
	}

	public void addSubsetURI(String subsetURI) {
		if (!subsetsURIs.contains(subsetURI) && subsetURI!=null)
			subsetsURIs.add(subsetURI);
	}

	public void removeSubsetURI(String subsetURI) {
		if (subsetsURIs.contains(subsetURI) && subsetURI!=null)
			subsetsURIs.remove(subsetURI);
	}
	
	public void addDistributionURI(String distributionURI) {
		if (!distributionsURIs.contains(distributionURI) && distributionURI!=null)
			distributionsURIs.add(distributionURI);
	}

	public void setLabel(String label) {
		this.label = label;
		mongoDBObject.put(LABEL, label);
	}

	public List<String> getDistributionsURIs() {
		return distributionsURIs;
	}

	public List<String> getSubsetsURIs() {
		return subsetsURIs;
	}

	public String getLabel() {
		return label;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDescriptionFileName() {
		return descriptionFileName;
	}

	public void setDescriptionFileName(String descriptionFileName) {
		this.descriptionFileName = descriptionFileName;
	}
	
	public Boolean getIsVocabulary() {
		return isVocabulary;
	}
	
	public void setIsVocabulary(Boolean isVocabulary) {
		this.isVocabulary = isVocabulary;
	}
	
	public ArrayList<String> getParentDatasetURI() {
		return parentDatasetsURI;
	}
	public void addParentDatasetURI(String parentDatasetURI) {
		if (!parentDatasetsURI.contains(parentDatasetURI) && parentDatasetURI!=null)
			parentDatasetsURI.add(parentDatasetURI);
	}

	public void removeParentDatasetURI(String parentDatasetURI) {
		if (parentDatasetsURI.contains(parentDatasetURI) && parentDatasetURI!=null)
			parentDatasetsURI.remove(parentDatasetURI);
	}

	public String getAccess_url() {
		return access_url;
	}

	public void setAccess_url(String access_url) {
		this.access_url = access_url;
	}
	
	public int getDynLodID() {
		return dynLodID;
	}

}
