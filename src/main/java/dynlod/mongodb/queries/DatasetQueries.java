package dynlod.mongodb.queries;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dynlod.mongodb.DataIDDB;
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;

public class DatasetQueries {
	
	static long triples = 0; 
	
	public static long getNumberOfTriples(String dataset){
		triples=0;
		getNumberOfTriples(new DatasetMongoDBObject(dataset));
		return triples;
	}
	public static long getNumberOfTriples(DatasetMongoDBObject dataset){
		triples=0;
		getTriples(dataset);
		return triples;
	}

	private static void getTriples(DatasetMongoDBObject dataset){
	
		for (String subset : dataset.getSubsetsURIs()) {
			getTriples(new DatasetMongoDBObject(subset));
		}
		
		for (String dist : dataset.getDistributionsURIs()) {
			DistributionMongoDBObject d= new DistributionMongoDBObject(dist);
			triples=triples+d.getTriples();
		}
	}
	
	// return number of datasets (vocabularies and not)
	public static int getNumberOfDatasets() {
		int numberOfDatasets = 0;
		try {
			DBCollection collection = DataIDDB.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			numberOfDatasets = (int) collection.count();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return numberOfDatasets;
	}
	
	// return all datasets
	public static ArrayList<DatasetMongoDBObject> getDatasets() {

		ArrayList<DatasetMongoDBObject> list = new ArrayList<DatasetMongoDBObject>();
		try {
			DBCollection collection = DataIDDB.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			DBCursor instances = collection.find();

			for (DBObject instance : instances) {
				list.add(new DatasetMongoDBObject(instance.get(DataIDDB.URI)
						.toString()));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	// return all datasets
	public static ArrayList<DatasetMongoDBObject> getDatasetsNotVocab() {

		ArrayList<DatasetMongoDBObject> list = new ArrayList<DatasetMongoDBObject>();
		try {
			DBCollection collection = DataIDDB.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(DatasetMongoDBObject.IS_VOCABULARY, false);
//			query.append("$where", "this.distributions_uris.length > 0");
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new DatasetMongoDBObject(instance.get(DataIDDB.URI)
						.toString()));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	// return all datasets
	public static ArrayList<DatasetMongoDBObject> getDatasetsVocab() {

		ArrayList<DatasetMongoDBObject> list = new ArrayList<DatasetMongoDBObject>();
		try {
			DBCollection collection = DataIDDB.getInstance().getCollection(
					DatasetMongoDBObject.COLLECTION_NAME);
			BasicDBObject query = new BasicDBObject(DatasetMongoDBObject.IS_VOCABULARY, true);
//			query.append("$where", "this.distributions_uris.length > 0");
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new DatasetMongoDBObject(instance.get(DataIDDB.URI)
						.toString()));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
}
