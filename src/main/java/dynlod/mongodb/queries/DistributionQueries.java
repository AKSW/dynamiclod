package dynlod.mongodb.queries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dynlod.linksets.DistributionFQDN;
import dynlod.mongodb.DBSuperClass;
import dynlod.mongodb.collections.DatasetDB;
import dynlod.mongodb.collections.DistributionDB;
import dynlod.mongodb.collections.DistributionObjectDomainsDB;
import dynlod.mongodb.collections.DistributionSubjectDomainsDB;

public class DistributionQueries {

	final static Logger logger = Logger.getLogger(DistributionQueries.class);

	public ArrayList<DistributionDB> getDistributionsByOutdegree(
			ArrayList<String> fqdnToSearch,
			ConcurrentHashMap<Integer, DistributionFQDN> fqdnPerDistribution) {
		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();
		try {

			// query all fqdn
			BasicDBObject query = new BasicDBObject(
					DistributionSubjectDomainsDB.SUBJECT_FQDN,
					new BasicDBObject("$in", fqdnToSearch));

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionSubjectDomainsDB.COLLECTION_NAME);

			DBCursor cursor = collection.find(query);

			// save a list with distribution and fqdn
			while (cursor.hasNext()) {
				DBObject instance = cursor.next();
				DistributionDB distribution = new DistributionDB(
						((Number) instance
								.get(DistributionSubjectDomainsDB.DISTRIBUTION_ID))
								.intValue());
				list.add(distribution);

				if (!fqdnPerDistribution
						.containsKey(distribution.getDynLodID())) {
					fqdnPerDistribution.put(distribution.getDynLodID(),
							createDistributionFQDNObject(distribution
									.getDynLodID()));

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public DistributionFQDN createDistributionFQDNObject(int distribution) {

		DistributionFQDN distributionFQDNObject = new DistributionFQDN();

		// query all objects fqdn for the distribution
		BasicDBObject subjectQuery = new BasicDBObject(
				DistributionSubjectDomainsDB.DISTRIBUTION_ID,
				distribution);

		DBCollection collection = DBSuperClass.getInstance().getCollection(
				DistributionSubjectDomainsDB.COLLECTION_NAME);

		DBCursor cursor = collection.find(subjectQuery);

		TreeSet<String> subjectsFQDN = new TreeSet<String>();

		while (cursor.hasNext()) {
			subjectsFQDN.add(cursor.next()
					.get(DistributionSubjectDomainsDB.SUBJECT_FQDN)
					.toString());
		}

		// doing the same for objects fqdn
		BasicDBObject objectQuery = new BasicDBObject(
				DistributionObjectDomainsDB.DISTRIBUTION_ID,
				distribution);

		collection = DBSuperClass.getInstance().getCollection(
				DistributionObjectDomainsDB.COLLECTION_NAME);

		cursor = collection.find(objectQuery);

		TreeSet<String> objectsFQDN = new TreeSet<String>();

		while (cursor.hasNext()) {
			objectsFQDN.add(cursor.next()
					.get(DistributionObjectDomainsDB.OBJECT_FQDN)
					.toString());
		}

		distributionFQDNObject.addObjectsFQDN(objectsFQDN);
		distributionFQDNObject.addSubjectsFQDN(subjectsFQDN);
		distributionFQDNObject.distributionMongoDBObject = new DistributionDB(
				distribution);
		distributionFQDNObject.distribution = distributionFQDNObject.distributionMongoDBObject
				.getDynLodID();

		return distributionFQDNObject;

	}

	public ArrayList<DistributionDB> getDistributionsByIndegree(
			ArrayList<String> fqdnToSearch,
			ConcurrentHashMap<Integer, DistributionFQDN> fqdnPerDistribution) {
		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();
		try {

			// find distributions with subjects
			BasicDBObject query = new BasicDBObject(
					DistributionObjectDomainsDB.OBJECT_FQDN,
					new BasicDBObject("$in", fqdnToSearch));

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionObjectDomainsDB.COLLECTION_NAME);

			DBCursor cursor = collection.find(query);

			while (cursor.hasNext()) {
				DBObject instance = cursor.next();
				DistributionDB distribution = new DistributionDB(
						((Number) instance
								.get(DistributionObjectDomainsDB.DISTRIBUTION_ID))
								.intValue());

				list.add(distribution);

				if (!fqdnPerDistribution.containsKey(distribution.getUri())) {
					fqdnPerDistribution.put(distribution.getDynLodID(),
							createDistributionFQDNObject(distribution
									.getDynLodID()));
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * 
	 * @return number of total triples read
	 */
	public int getNumberOfTriples() {
		int numberOfTriples = 0;
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionDB.COLLECTION_NAME);

			BasicDBObject select = new BasicDBObject("$match",
					new BasicDBObject(
							DistributionDB.SUCCESSFULLY_DOWNLOADED,
							true));

			BasicDBObject groupFields = new BasicDBObject("_id", null);

			groupFields.append("sum", new BasicDBObject("$sum", "$triples"));

			DBObject group = new BasicDBObject("$group", groupFields);

			// run aggregation
			List<DBObject> pipeline = Arrays.asList(select, group);
			AggregationOutput output = collection.aggregate(pipeline);

			for (DBObject result : output.results()) {
				numberOfTriples = Integer.valueOf(result.get("sum").toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return numberOfTriples;
	}

	/**
	 * Get distributions
	 * 
	 * @param withVocabularies
	 *            specifies whether should vocabularies are added to the return
	 *            list
	 * @return a ArrayList of DistributionMongoDBObject
	 */
	public ArrayList<DistributionDB> getDistributions(
			boolean withVocabularies) {

		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionDB.COLLECTION_NAME);
			DBCursor instances = collection.find();

			for (DBObject instance : instances) {
				list.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * Get distributions in a range
	 * 
	 * @param skip
	 *            initial value of range
	 * @param limit
	 *            final value of range
	 * @return a ArrayList of DistributionMongoDBObject
	 */
	public ArrayList<DistributionDB> getDistributions(int skip,
			int limit, boolean isVocabulary, String search) {

		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionDB.COLLECTION_NAME);

			DBObject query;
			query = new BasicDBObject(DistributionDB.IS_VOCABULARY,
					isVocabulary);

			if (search != null) {
				DBObject query2;
				DBObject query3;
//				System.out.println(search);
//				System.out.println(isVocabulary);
				query3 = new BasicDBObject(DistributionDB.IS_VOCABULARY,
						isVocabulary);
//				query2 = new BasicDBObject(DistributionMongoDBObject.DOWNLOAD_URL, new BasicDBObject("$regex",""+search+""));
				query2 = new BasicDBObject(DistributionDB.DOWNLOAD_URL, java.util.regex.Pattern.compile(search));
				
//				DatasetMongoDBObject.URI, /.*m.*/
//				new BasicDBObject("$regex", topDataset + ".*")
				
				
				
				// make a AND operator
				BasicDBList and = new BasicDBList();
				and.add(query3);
				and.add(query2);
				query = new BasicDBObject("$and", and);
			}

//			DBCursor inst = collection.find(new BasicDBObject("$and", and));

			DBCursor instances = collection.find(query).skip(skip).limit(limit);

			for (DBObject instance : instances) {
				list.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * Count distributions
	 * 
	 * @param withVocabularies
	 *            parameter that set whether vocabularies should be included in
	 *            the result
	 * @return number of distributions
	 */
	public int countDistributions(boolean withVocabularies) {

		ArrayList<DistributionDB> list = new ArrayList<DistributionDB>();

		DBCursor instances;

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionDB.COLLECTION_NAME);

			DBObject query;

			query = new BasicDBObject(DistributionDB.IS_VOCABULARY,
					withVocabularies);

			instances = collection.find(query);

			return instances.count();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
	}

	// return all distributions
	public ArrayList<DistributionDB> getDistributionsByTopDatasetID(
			String topDataset) {

		ArrayList<DistributionDB> distributionList = new ArrayList<DistributionDB>();

		ArrayList<Integer> datasetList = new ArrayList<Integer>();

		DBCollection collection = DBSuperClass.getInstance().getCollection(
				DatasetDB.COLLECTION_NAME);

		// find address by URI...
		BasicDBObject uriQuery = new BasicDBObject(DatasetDB.URI,
				new BasicDBObject("$regex", topDataset + ".*"));

		// ... or by access url
		BasicDBObject accessQuery = new BasicDBObject(
				DatasetDB.ACCESS_URL, new BasicDBObject("$regex",
						topDataset + ".*"));

		// make a OR operator
		BasicDBList or = new BasicDBList();
		or.add(uriQuery);
		or.add(accessQuery);

		DBCursor inst = collection.find(new BasicDBObject("$or", or));

		while (inst.hasNext()) {
			datasetList.add(((Number) inst.next().get(
					DatasetDB.DYN_LOD_ID)).intValue());
		}

		try {
			collection = DBSuperClass.getInstance().getCollection(
					DistributionDB.COLLECTION_NAME);
			DBCursor instances = collection.find(new BasicDBObject(
					DistributionDB.DEFAULT_DATASETS,
					new BasicDBObject("$in", datasetList)));

			for (DBObject instance : instances) {
				distributionList.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return distributionList;
	}

	// return all distributions
	public ArrayList<DistributionDB> getSetOfDistributions(
			HashSet<Integer> set) {

		ArrayList<DistributionDB> distributionList = new ArrayList<DistributionDB>();

		DBCollection collection = DBSuperClass.getInstance().getCollection(
				DatasetDB.COLLECTION_NAME);

		try {
			collection = DBSuperClass.getInstance().getCollection(
					DistributionDB.COLLECTION_NAME);
			DBCursor instances = collection.find(new BasicDBObject(
					DistributionDB.DYN_LOD_ID, new BasicDBObject(
							"$in", set)));

			for (DBObject instance : instances) {
				distributionList.add(new DistributionDB(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return distributionList;
	}

}
