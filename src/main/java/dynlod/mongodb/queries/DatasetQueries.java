package dynlod.mongodb.queries;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dynlod.mongodb.DBSuperClass;
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.DistributionSubjectDomainsMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;

public class DatasetQueries {

	static long triples = 0;

	public long getNumberOfTripless(String dataset) {
		triples = 0;
		getNumberOfTriples(new DatasetMongoDBObject(dataset));
		return triples;
	}

	public long getNumberOfTriples(DatasetMongoDBObject dataset) {
		triples = 0;
		getTriples(dataset);
		return triples;
	}

	private void getTriples(DatasetMongoDBObject dataset) {

		for (int subset : dataset.getSubsetsIDs()) {
			getTriples(new DatasetMongoDBObject(subset));
		}

		for (int dist : dataset.getDistributionsIDs()) {
			DistributionMongoDBObject d = new DistributionMongoDBObject(dist);
			triples = triples + d.getTriples();
		}
	}

	// return number of datasets (vocabularies and not)
	public int getNumberOfDatasets() {
		int numberOfDatasets = 0;
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			numberOfDatasets = (int) collection.count();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return numberOfDatasets;
	}

	// return all datasets
	public ArrayList<DatasetMongoDBObject> getDatasets() {

		ArrayList<DatasetMongoDBObject> list = new ArrayList<DatasetMongoDBObject>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			DBCursor instances = collection.find();

			for (DBObject instance : instances) {
				list.add(new DatasetMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	// get an array of datasets
	public ArrayList<DatasetMongoDBObject> getDatasets(ArrayList<Integer> datasetsIDs) {

		ArrayList<DatasetMongoDBObject> list = new ArrayList<DatasetMongoDBObject>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject();
			query.put(DatasetMongoDBObject.DYN_LOD_ID, new BasicDBObject("$in", datasetsIDs));
			
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new DatasetMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// return all datasets
	public ArrayList<DatasetMongoDBObject> getDatasetsNotVocab() {
//		public ArrayList<DatasetMongoDBObject> getDatasetsNotVocab() {

		ArrayList<DatasetMongoDBObject> list = new ArrayList<DatasetMongoDBObject>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(
					DatasetMongoDBObject.IS_VOCABULARY, false);
			DBCursor instances = collection.find(query).sort(
					new BasicDBObject(DatasetMongoDBObject.TITLE, 1));

			for (DBObject instance : instances) {
				list.add(new DatasetMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	// return all datasets
	public ArrayList<DatasetMongoDBObject> getTopDatasetsNotVocab() {
//		public ArrayList<DatasetMongoDBObject> getDatasetsNotVocab() {

		ArrayList<DatasetMongoDBObject> list = new ArrayList<DatasetMongoDBObject>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(
					DatasetMongoDBObject.IS_VOCABULARY, false);
			ArrayList<Integer> topDatasetID = new ArrayList<Integer>();
			topDatasetID.add(0);
			query.append(DatasetMongoDBObject.PARENT_DATASETS, new BasicDBObject("$in", topDatasetID));
			DBCursor instances = collection.find(query).sort(
					new BasicDBObject(DatasetMongoDBObject.TITLE, 1));

			for (DBObject instance : instances) {
				if(((ArrayList<String>) instance.get(DatasetMongoDBObject.PARENT_DATASETS)).size()==1){
					list.add(new DatasetMongoDBObject(instance));
					
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	
	// return all datasets
	public ArrayList<DatasetMongoDBObject> getDatasetsByID(ArrayList<Integer> ids) {
//		public ArrayList<DatasetMongoDBObject> getDatasetsNotVocab() {

		ArrayList<DatasetMongoDBObject> list = new ArrayList<DatasetMongoDBObject>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(
					DatasetMongoDBObject.IS_VOCABULARY, false);
			DBCursor instances = collection.find(query).sort(
					new BasicDBObject(DatasetMongoDBObject.TITLE, 1));

			for (DBObject instance : instances) {
				list.add(new DatasetMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// return all datasets not vocabularies with links

	// public void getDatasetsNotVocabWithLinks() {
	public ArrayList<DatasetMongoDBObject> getDatasetsNotVocabWithLinks() {

		ArrayList<DatasetMongoDBObject> list = new ArrayList<DatasetMongoDBObject>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(LinksetMongoDBObject.LINKS,
					new BasicDBObject("$gt", 50));
			List<Integer> out = collection.distinct(
					LinksetMongoDBObject.DATASET_TARGET, query);

			List<Integer> in = collection.distinct(
					LinksetMongoDBObject.DATASET_SOURCE, query);

			TreeSet<Integer> t = new TreeSet<Integer>();
			for (Integer s : out) {
				t.add(s);
			}
			for (Integer s : in) {
				t.add(s);
			}

			collection = DBSuperClass.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			query = new BasicDBObject(DatasetMongoDBObject.DYN_LOD_ID,
					new BasicDBObject("$in", t));
			query.append(DatasetMongoDBObject.IS_VOCABULARY, false);

			DBCursor instances = collection.find(query).sort(
					new BasicDBObject(DatasetMongoDBObject.TITLE, 1));

			for (DBObject instance : instances) {
				list.add(new DatasetMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// return all datasets
	public ArrayList<DatasetMongoDBObject> getDatasetsVocab() {

		ArrayList<DatasetMongoDBObject> list = new ArrayList<DatasetMongoDBObject>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(
					DatasetMongoDBObject.IS_VOCABULARY, true);
			// query.append("$where", "this.distributions_uris.length > 0");
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new DatasetMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public ArrayList<DatasetMongoDBObject> getSubsetsAsMongoDBObject(DatasetMongoDBObject dataset) {

		ArrayList<DatasetMongoDBObject> list = new ArrayList<DatasetMongoDBObject>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(
					DatasetMongoDBObject.DYN_LOD_ID, new BasicDBObject("$in", dataset.getSubsetsIDs()));
			// query.append("$where", "this.distributions_uris.length > 0");
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new DatasetMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public ArrayList<DistributionMongoDBObject> getDistributionsAsMongoDBObject(DatasetMongoDBObject dataset) {

		ArrayList<DistributionMongoDBObject> list = new ArrayList<DistributionMongoDBObject>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionMongoDBObject.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(
					DistributionMongoDBObject.DYN_LOD_ID, new BasicDBObject("$in", dataset.getDistributionsIDs()));
			// query.append("$where", "this.distributions_uris.length > 0");
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new DistributionMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

//	public DatasetMongoDBObject getDatasetById(int id) {
//		DBCollection collection = DBSuperClass.getInstance().getCollection(
//				DatasetMongoDBObject.COLLECTION_NAME);
//		BasicDBObject query = new BasicDBObject(
//				DatasetMongoDBObject.DYN_LOD_ID, id);
//		// query.append("$where", "this.distributions_uris.length > 0");
//		DBCursor instances = collection.find(query);
//
//		if (instances.hasNext())
//			return new DatasetMongoDBObject(instances.next()
//					.get(DatasetMongoDBObject.URI).toString());
//		else
//			return null;
//	}

}
