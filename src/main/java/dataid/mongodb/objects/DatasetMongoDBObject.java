package dataid.mongodb.objects;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

import dataid.exceptions.DataIDException;
import dataid.mongodb.DataIDDB;

public class DatasetMongoDBObject extends DataIDDB {
	 
	// Collection name
	public static final String COLLECTION_NAME = "Dataset";

	public static final String LABEL = "label";

	public static final String TITLE = "title";
	
	public static final String PARENT_DATASETS = "parent_datasets";

	public static final String SUBSET_URIS = "subset_uris";

	public static final String DISTRIBUTIONS_URIS = "distributions_uris";

	public static final String DATAID_FILENAME = "dataid_file_name";
	
	public static final String OBJECT_FILENAME = "object_file_name";
	
	public static final String SUBJECT_FILTER_FILENAME = "subject_file_name";
	
	public static final String IS_VOCABULARY = "is_vocabulary";
	
	

	// class properties

	private String label;

	private String title;

	private String dataIdFileName;
	
	private boolean isVocabulary = false;
	

	private ArrayList<String> subsetsURIs = new ArrayList<String> ();

	private ArrayList<String>  distributionsURIs = new ArrayList<String> ();
	
	private ArrayList<String> parentDatasetsURI = new ArrayList<String>();

	public DatasetMongoDBObject(String uri) {
		super(COLLECTION_NAME, uri);
		loadObject();
	}

	public boolean updateObject(boolean checkBeforeInsert) {

		// save object case it doens't exists
		try {
			// updating subsets on mongodb
			mongoDBObject.put(SUBSET_URIS, subsetsURIs);
			
			// updating distributions on mongodb
			mongoDBObject.put(DISTRIBUTIONS_URIS, distributionsURIs);
	
			mongoDBObject.put(TITLE, title);

			mongoDBObject.put(DATAID_FILENAME, dataIdFileName);

			mongoDBObject.put(LABEL, label);
			
			mongoDBObject.put(IS_VOCABULARY, isVocabulary);
			
			mongoDBObject.put(PARENT_DATASETS, parentDatasetsURI);
			
			
			insert(checkBeforeInsert);
			return true;
		} catch (Exception e2) {
//			e2.printStackTrace();

			try {
				if (update())
					return true;
				else
					return false;
			} catch (DataIDException e) {
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
			title = (String) obj.get(TITLE);
			dataIdFileName = (String) obj.get(DATAID_FILENAME);
			isVocabulary = (Boolean) obj.get(IS_VOCABULARY);

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

	public String getDataIdFileName() {
		return dataIdFileName;
	}

	public void setDataIdFileName(String dataIdFileName) {
		this.dataIdFileName = dataIdFileName;
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
	

}
