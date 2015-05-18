package dynlod.mongodb.queries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dynlod.mongodb.DataIDDB;
import dynlod.mongodb.objects.LinksetMongoDBObject;

public class LinksetQueries {

	public static ArrayList<LinksetMongoDBObject> getLinksets() {

		ArrayList<LinksetMongoDBObject> list = new ArrayList<LinksetMongoDBObject>();

		try {
			DBCollection collection = DataIDDB.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);
			DBCursor instances = collection.find();

			for (DBObject instance : instances) {
				list.add(new LinksetMongoDBObject(instance.get(DataIDDB.URI)
						.toString()));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public static ArrayList<LinksetMongoDBObject> getLinksetsWithLinks() {

		ArrayList<LinksetMongoDBObject> list = new ArrayList<LinksetMongoDBObject>();

		try {
			DBCollection collection = DataIDDB.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);
			DBObject query = new BasicDBObject(LinksetMongoDBObject.LINKS,
					new BasicDBObject("$gt", 50));
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new LinksetMongoDBObject(instance.get(DataIDDB.URI)
						.toString()));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// @Test
	// public void getLinksetsGroupByDatasets() {
	public static ArrayList<LinksetMongoDBObject> getLinksetsGroupByDatasets() {
		AggregationOutput output;
		try {

			DBCollection collection = DataIDDB.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);

			// Now the $group operation
			DBObject groupFields = new BasicDBObject("_id", new BasicDBObject(
					"objectsDatasetTarget", "$objectsDatasetTarget").append(
					"subjectsDatasetTarget", "$subjectsDatasetTarget"));
			groupFields.put("id", new BasicDBObject("$first", "$_id"));

			DBObject group = new BasicDBObject("$group", groupFields);

			// run aggregation
			List<DBObject> pipeline = Arrays.asList(group);
			output = collection.aggregate(pipeline);

			// for (DBObject result : output.results()) {
			// System.out.println(result);
			// }
			ArrayList<LinksetMongoDBObject> linksetList = new ArrayList<LinksetMongoDBObject>();

			for (DBObject result : output.results()) {
				linksetList.add(new LinksetMongoDBObject(result.get("id")
						.toString()));
			}

			return linksetList;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	

	public static ArrayList<LinksetMongoDBObject> getLinksetsGroupByDistributions() {

		try {
			ArrayList<LinksetMongoDBObject> list = new ArrayList<LinksetMongoDBObject>();

			DBCollection collection = DataIDDB.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);
			DBCursor instances = collection.find();

			for (DBObject instance : instances) {
				list.add(new LinksetMongoDBObject(instance.get(DataIDDB.URI)
						.toString()));
			}
		
			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	

	public static ArrayList<LinksetMongoDBObject> getLinksetsFilterByDataset(
			String dataset) {

		try {
			ArrayList<LinksetMongoDBObject> list = new ArrayList<LinksetMongoDBObject>();

			DBCollection collection = DataIDDB.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);

			ArrayList<BasicDBObject> a = new ArrayList<BasicDBObject>();
			a.add(new BasicDBObject(
					LinksetMongoDBObject.OBJECTS_DATASET_TARGET, dataset));
			a.add(new BasicDBObject(
					LinksetMongoDBObject.SUBJECTS_DATASET_TARGET, dataset));

			BasicDBObject or = new BasicDBObject(new BasicDBObject("$and", a));

			DBCursor instances = collection.find(or);

			for (DBObject instance : instances) {
				list.add(new LinksetMongoDBObject(instance.get(DataIDDB.URI)
						.toString()));
			}

			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static ArrayList<LinksetMongoDBObject> getLinksetsInDegreeByDistribution(
			String url) {
		ArrayList<LinksetMongoDBObject> list = new ArrayList<LinksetMongoDBObject>();
		try {

			DBCollection collection = DataIDDB.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);
			
			DBObject clause1 = new BasicDBObject(LinksetMongoDBObject.SUBJECTS_DISTRIBUTION_TARGET, new BasicDBObject("$regex", url+".*"));  
			DBObject clause2 = new BasicDBObject(LinksetMongoDBObject.LINKS,
					new BasicDBObject("$gt", 50));   

			BasicDBList or = new BasicDBList();
			or.add(clause1);
			or.add(clause2);
			DBObject query = new BasicDBObject("$and", or);
			DBCursor d = collection.find(query);

			while (d.hasNext()) {
				list.add(new LinksetMongoDBObject(d.next().get(DataIDDB.URI)
						.toString()));
			}

		
			return list;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static ArrayList<LinksetMongoDBObject> getLinksetsOutDegreeByDistribution(
			String url) {
		ArrayList<LinksetMongoDBObject> list = new ArrayList<LinksetMongoDBObject>();
		try {

			DBCollection collection = DataIDDB.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);
			
			DBObject clause1 = new BasicDBObject(LinksetMongoDBObject.OBJECTS_DISTRIBUTION_TARGET, new BasicDBObject("$regex", url+".*"));  
			DBObject clause2 = new BasicDBObject(LinksetMongoDBObject.LINKS,
					new BasicDBObject("$gt", 50));   

			BasicDBList and = new BasicDBList();
			and.add(clause1);
			and.add(clause2);
			DBObject query = new BasicDBObject("$and", and);
			
			DBCursor d = collection.find(query);

			while (d.hasNext()) {
				list.add(new LinksetMongoDBObject(d.next().get(DataIDDB.URI)
						.toString()));
			}

			return list;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	

	public static ArrayList<LinksetMongoDBObject> getLinksetsInDegreeByDataset(
			String url) {
		ArrayList<LinksetMongoDBObject> list = new ArrayList<LinksetMongoDBObject>();
		try {

			DBCollection collection = DataIDDB.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);
			
			DBObject clause1 = new BasicDBObject(LinksetMongoDBObject.SUBJECTS_DATASET_TARGET,  new BasicDBObject("$regex", url+".*"));  
			DBObject clause2 = new BasicDBObject(LinksetMongoDBObject.LINKS,
					new BasicDBObject("$gt", 50));   

			BasicDBList or = new BasicDBList();
			or.add(clause1);
			or.add(clause2);
			DBObject query = new BasicDBObject("$and", or);
			DBCursor d = collection.find(query);

			while (d.hasNext()) {
				list.add(new LinksetMongoDBObject(d.next().get(DataIDDB.URI)
						.toString()));
			}

			return list;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static ArrayList<LinksetMongoDBObject> getLinksetsOutDegreeByDataset(
			String url) {
		ArrayList<LinksetMongoDBObject> list = new ArrayList<LinksetMongoDBObject>();
		try {

			DBCollection collection = DataIDDB.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);
			
			DBObject clause1 = new BasicDBObject(LinksetMongoDBObject.OBJECTS_DATASET_TARGET, new BasicDBObject("$regex", url+".*"));  
			DBObject clause2 = new BasicDBObject(LinksetMongoDBObject.LINKS,
					new BasicDBObject("$gt", 50));   

			BasicDBList and = new BasicDBList();
			and.add(clause1);
			and.add(clause2);
			DBObject query = new BasicDBObject("$and", and);
			
			DBCursor d = collection.find(query);

			while (d.hasNext()) {
				list.add(new LinksetMongoDBObject(d.next().get(DataIDDB.URI)
						.toString()));
			}

			return list;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static boolean isOnLinksetList(String downloadURLObject,
			String downloladURLSubject) {

		DBCollection collection = DataIDDB.getInstance().getCollection(
				LinksetMongoDBObject.COLLECTION_NAME);
		BasicDBObject query = new BasicDBObject(
				LinksetMongoDBObject.SUBJECTS_DISTRIBUTION_TARGET,
				downloladURLSubject);
		query.append(LinksetMongoDBObject.OBJECTS_DISTRIBUTION_TARGET,
				downloadURLObject);

		DBCursor d = collection.find(query);

		if (d.hasNext()) {
			return true;
		}

		return false;
	}
	
	public static boolean checkIfDatasetExists(String datasetURL) {

		DBCollection collection = DataIDDB.getInstance().getCollection(
				LinksetMongoDBObject.COLLECTION_NAME);
		BasicDBObject clause1 = new BasicDBObject(
				LinksetMongoDBObject.SUBJECTS_DATASET_TARGET,
				new BasicDBObject("$regex", datasetURL+".*"));
		BasicDBObject clause2 = new BasicDBObject(LinksetMongoDBObject.OBJECTS_DATASET_TARGET,
				new BasicDBObject("$regex", datasetURL+".*"));
		
		BasicDBList or = new BasicDBList();
		or.add(clause1);
		or.add(clause2);
		DBObject query = new BasicDBObject("$or", or);
		
		DBCursor d = collection.find(query).limit(1);
		

		if (d.hasNext()) {
			return true;
		}

		return false;
	}
	
	public static boolean checkIfDistributionExists(String distributionURL) {

		DBCollection collection = DataIDDB.getInstance().getCollection(
				LinksetMongoDBObject.COLLECTION_NAME);
		BasicDBObject clause1 = new BasicDBObject(
				LinksetMongoDBObject.SUBJECTS_DISTRIBUTION_TARGET,
				new BasicDBObject("$regex", distributionURL+".*"));
		BasicDBObject clause2 = new BasicDBObject(LinksetMongoDBObject.OBJECTS_DISTRIBUTION_TARGET,
				 new BasicDBObject("$regex", distributionURL+".*"));
		
		BasicDBList and = new BasicDBList();
		and.add(clause1);
		and.add(clause2);
		DBObject query = new BasicDBObject("$or", and);
		
		DBCursor d = collection.find(query).limit(1);

		if (d.hasNext()) {
			return true;
		}

		return false;
	}
	
	

}