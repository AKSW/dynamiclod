package dynlod.mongodb.queries;

import java.util.ArrayList;
import java.util.Arrays;
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
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.DistributionObjectDomainsMongoDBObject;
import dynlod.mongodb.objects.DistributionSubjectDomainsMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;

public class DistributionQueries {

	final static Logger logger = Logger.getLogger(DistributionQueries.class);

	// return number of distributions
	public static int getNumberOfDistributions() {
		int numberOfDistributions = 0;
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionMongoDBObject.COLLECTION_NAME);
			numberOfDistributions = (int) collection.count();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return numberOfDistributions;
	}

	public static ArrayList<DistributionMongoDBObject> getDistributionsByOutdegree(
			String distributionAccessURL) {
		ArrayList<DistributionMongoDBObject> list = new ArrayList<DistributionMongoDBObject>();
		try {

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionObjectDomainsMongoDBObject.COLLECTION_NAME);

			// get all objects domain of a distribution
			BasicDBObject query = new BasicDBObject(
					DistributionObjectDomainsMongoDBObject.DISTRIBUTION_ID,
					distributionAccessURL);

			BasicDBObject fields = new BasicDBObject(
					DistributionObjectDomainsMongoDBObject.OBJECT_FQDN, 1);
			fields.append("_id", 0);
			DBCursor cursor = collection.find(query, fields);

			ArrayList<String> vals = new ArrayList<String>();
			while (cursor.hasNext()) {
				vals.add((String) cursor.next().get(
						DistributionObjectDomainsMongoDBObject.OBJECT_FQDN));
			}

			BasicDBObject fields2 = new BasicDBObject(
					DistributionSubjectDomainsMongoDBObject.DISTRIBUTION_ID, 1);
			fields2.append("_id", 0);

			// find distributions with contains subjects equal of objects (vals)
			BasicDBObject query2 = new BasicDBObject(
					DistributionSubjectDomainsMongoDBObject.SUBJECT_FQDN,
					new BasicDBObject("$in", vals));

			collection = DBSuperClass.getInstance().getCollection(
					DistributionSubjectDomainsMongoDBObject.COLLECTION_NAME);

			cursor = collection.find(query2, fields2);

			while (cursor.hasNext()) {
				DistributionMongoDBObject obj = new DistributionMongoDBObject(
						cursor.next());

				list.add(obj);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public ArrayList<DistributionMongoDBObject> getDistributionsByOutdegree(
			ArrayList<String> fqdnToSearch,
			ConcurrentHashMap<Integer, DistributionFQDN> fqdnPerDistribution) {
		ArrayList<DistributionMongoDBObject> list = new ArrayList<DistributionMongoDBObject>();
		try {

			// query all fqdn
			BasicDBObject query = new BasicDBObject(
					DistributionSubjectDomainsMongoDBObject.SUBJECT_FQDN,
					new BasicDBObject("$in", fqdnToSearch));

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionSubjectDomainsMongoDBObject.COLLECTION_NAME);

			DBCursor cursor = collection.find(query);
			
			// save a list with distribution and fqdn
			while (cursor.hasNext()) {
				DBObject instance = cursor.next();
				DistributionMongoDBObject distribution =  new DistributionMongoDBObject(((Number) instance.get(DistributionSubjectDomainsMongoDBObject.DISTRIBUTION_ID)).intValue());
				list.add(distribution);


				if (!fqdnPerDistribution.containsKey(distribution.getDynLodID())) {
					fqdnPerDistribution
							.put(distribution.getDynLodID(),
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
				DistributionSubjectDomainsMongoDBObject.DISTRIBUTION_ID,
				distribution);

		DBCollection collection = DBSuperClass.getInstance().getCollection(
				DistributionSubjectDomainsMongoDBObject.COLLECTION_NAME);

		DBCursor cursor = collection.find(subjectQuery);

		TreeSet<String> subjectsFQDN = new TreeSet<String>();

		while (cursor.hasNext()) {
			subjectsFQDN.add(cursor.next()
					.get(DistributionSubjectDomainsMongoDBObject.SUBJECT_FQDN)
					.toString());
		}

		// doing the same for objects fqdn
		BasicDBObject objectQuery = new BasicDBObject(
				DistributionObjectDomainsMongoDBObject.DISTRIBUTION_ID,
				distribution);

		collection = DBSuperClass.getInstance().getCollection(
				DistributionObjectDomainsMongoDBObject.COLLECTION_NAME);

		cursor = collection.find(objectQuery);

		TreeSet<String> objectsFQDN = new TreeSet<String>();

		while (cursor.hasNext()) {
			objectsFQDN.add(cursor.next()
					.get(DistributionObjectDomainsMongoDBObject.OBJECT_FQDN)
					.toString());
		}

		distributionFQDNObject.addObjectsFQDN(objectsFQDN);
		distributionFQDNObject.addSubjectsFQDN(subjectsFQDN);
		distributionFQDNObject.distributionMongoDBObject =  new DistributionMongoDBObject(distribution);
		distributionFQDNObject.distribution = distributionFQDNObject.distributionMongoDBObject.getDynLodID();

		return distributionFQDNObject;

	}


	public ArrayList<DistributionMongoDBObject> getDistributionsByIndegree(
			ArrayList<String> fqdnToSearch, ConcurrentHashMap<Integer, DistributionFQDN> fqdnPerDistribution) {
		ArrayList<DistributionMongoDBObject> list = new ArrayList<DistributionMongoDBObject>();
		try {

			// find distributions with subjects
			BasicDBObject query2 = new BasicDBObject(
					DistributionObjectDomainsMongoDBObject.OBJECT_FQDN,
					new BasicDBObject("$in", fqdnToSearch));

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionObjectDomainsMongoDBObject.COLLECTION_NAME);

			DBCursor cursor = collection.find(query2);

			while (cursor.hasNext()) {
				DBObject instance = cursor.next();
				DistributionMongoDBObject distribution =  new DistributionMongoDBObject(((Number) instance.get(DistributionObjectDomainsMongoDBObject.DISTRIBUTION_ID)).intValue());

				list.add(distribution);

				if (!fqdnPerDistribution.containsKey(distribution.getUri())) {
					fqdnPerDistribution
							.put(distribution.getDynLodID(),
									createDistributionFQDNObject(distribution
											.getDynLodID()));
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public static ArrayList<DistributionMongoDBObject> getDistributionsByIndegree(
			String distributionAccessURL) {
		ArrayList<DistributionMongoDBObject> list = new ArrayList<DistributionMongoDBObject>();
		try {

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionSubjectDomainsMongoDBObject.COLLECTION_NAME);

			// get all subject domain from distribution got as parameter
			BasicDBObject query = new BasicDBObject(
					DistributionSubjectDomainsMongoDBObject.DISTRIBUTION_ID,
					distributionAccessURL);

			BasicDBObject fields = new BasicDBObject(
					DistributionSubjectDomainsMongoDBObject.SUBJECT_FQDN, 1);
			fields.append("_id", 0);
			DBCursor cursor = collection.find(query, fields);

			ArrayList<String> vals = new ArrayList<String>();
			while (cursor.hasNext()) {
				vals.add((String) cursor.next().get(
						DistributionSubjectDomainsMongoDBObject.SUBJECT_FQDN));
			}

			BasicDBObject fields2 = new BasicDBObject(
					DistributionObjectDomainsMongoDBObject.DISTRIBUTION_ID, 1);
			fields2.append("_id", 0);

			// find distributions with subjects
			BasicDBObject query2 = new BasicDBObject(
					DistributionObjectDomainsMongoDBObject.OBJECT_FQDN,
					new BasicDBObject("$in", vals));

			collection = DBSuperClass.getInstance().getCollection(
					DistributionObjectDomainsMongoDBObject.COLLECTION_NAME);

			cursor = collection.find(query2, fields2);

			while (cursor.hasNext()) {
				DistributionMongoDBObject obj = new DistributionMongoDBObject(
						cursor.next());

				list.add(obj);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// return number of triples
	public static int getNumberOfTriples() {
		int numberOfTriples = 0;
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionMongoDBObject.COLLECTION_NAME);

			BasicDBObject select = new BasicDBObject("$match",
					new BasicDBObject(
							DistributionMongoDBObject.SUCCESSFULLY_DOWNLOADED,
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

	// return all distributions
	public static ArrayList<DistributionMongoDBObject> getDistributions(
			boolean b) {

		ArrayList<DistributionMongoDBObject> list = new ArrayList<DistributionMongoDBObject>();

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionMongoDBObject.COLLECTION_NAME);
			DBCursor instances = collection.find();

			for (DBObject instance : instances) {
				list.add(new DistributionMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// return all distributions within an interval
	public static ArrayList<DistributionMongoDBObject> getDistributions(
			int skip, int limit, boolean getOntologies) {

		ArrayList<DistributionMongoDBObject> list = new ArrayList<DistributionMongoDBObject>();

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionMongoDBObject.COLLECTION_NAME);
			
			DBObject query;
			
			query = new BasicDBObject(DistributionMongoDBObject.IS_VOCABULARY, getOntologies);			
			
			
			DBCursor instances = collection.find(query).skip(skip).limit(limit);

			for (DBObject instance : instances) {
				list.add(new DistributionMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	// return all distributions
	public static int countDistributionsByVocabularies(
			boolean vocabularies) {

		ArrayList<DistributionMongoDBObject> list = new ArrayList<DistributionMongoDBObject>();
		
		DBCursor instances;

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionMongoDBObject.COLLECTION_NAME);
			
			DBObject query;
			
			query = new BasicDBObject(DistributionMongoDBObject.IS_VOCABULARY, vocabularies);			
			
			instances = collection.find(query); 

			return instances.count();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return 0;
	}
	
	

	// return all distributions
	public static ArrayList<DistributionMongoDBObject> getDistributionsByTopDatasetID(
			String topDataset) {

		ArrayList<DistributionMongoDBObject> distributionList = new ArrayList<DistributionMongoDBObject>();

		ArrayList<Integer> datasetList = new ArrayList<Integer>();

		DBCollection collection = DBSuperClass.getInstance().getCollection(
				DatasetMongoDBObject.COLLECTION_NAME);
		DBCursor inst = collection.find(new BasicDBObject(
				DatasetMongoDBObject.ACCESS_URL, new BasicDBObject("$regex",
						topDataset + ".*")));
		while (inst.hasNext()) {
			datasetList.add(((Number)inst.next().get(DatasetMongoDBObject.DYN_LOD_ID)).intValue());
		}

		try {
			collection = DBSuperClass.getInstance().getCollection(
					DistributionMongoDBObject.COLLECTION_NAME);
			DBCursor instances = collection.find(new BasicDBObject(
					DistributionMongoDBObject.DEFAULT_DATASETS,
					new BasicDBObject("$in", datasetList)));

			for (DBObject instance : instances) {
				distributionList.add(new DistributionMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return distributionList;
	}

	public static ArrayList<DistributionMongoDBObject> getDistributionsWithErrors() {

		ArrayList<DistributionMongoDBObject> list = new ArrayList<DistributionMongoDBObject>();

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionMongoDBObject.COLLECTION_NAME);

			DBObject clause1 = new BasicDBObject(
					DistributionMongoDBObject.STATUS,
					DistributionMongoDBObject.STATUS_CREATING_LINKSETS);
			DBObject clause2 = new BasicDBObject(
					DistributionMongoDBObject.STATUS,
					DistributionMongoDBObject.STATUS_WAITING_TO_STREAM);
			BasicDBList or = new BasicDBList();
			or.add(clause1);
			or.add(clause2);
			DBObject query = new BasicDBObject("$or", or);

			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(new DistributionMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	

	public static DistributionMongoDBObject getByDownloadURL(String url) {

		DistributionMongoDBObject dist = null;
		try {

			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionMongoDBObject.COLLECTION_NAME);

			BasicDBObject query = new BasicDBObject(
					DistributionMongoDBObject.DOWNLOAD_URL, url);
			DBCursor d = collection.find(query);

			if (d.hasNext()) {
				dist = new DistributionMongoDBObject(d.next());
				return dist;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	// return all distributions
	public static ArrayList<DistributionMongoDBObject> getDistributionsWithLinks() {

		ArrayList<DistributionMongoDBObject> list = new ArrayList<DistributionMongoDBObject>();
		ArrayList<String> distributions = new ArrayList<String>();

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionMongoDBObject.COLLECTION_NAME);
			DBCollection collection2 = DBSuperClass.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);
			DBCursor instances = collection.find();

			for (DBObject instance : instances) {
				BasicDBObject query = new BasicDBObject(
						LinksetMongoDBObject.DISTRIBUTION_TARGET, instance.get(
								DistributionMongoDBObject.DOWNLOAD_URL)
								.toString());
				DBCursor d = collection2.find(query);

				if (d.hasNext()) {
					if (!distributions.contains(instance.get(
							DistributionMongoDBObject.DOWNLOAD_URL).toString())) {
						distributions.add(instance.get(
								DistributionMongoDBObject.DOWNLOAD_URL)
								.toString());
						// System.out.println(instance.get(DistributionMongoDBObject.DOWNLOAD_URL).toString());
					}
				}
			}

			for (DBObject instance : instances) {

				BasicDBObject query = new BasicDBObject(
						LinksetMongoDBObject.DISTRIBUTION_SOURCE, instance.get(
								DistributionMongoDBObject.DOWNLOAD_URL)
								.toString());
				DBCursor d = collection2.find(query);

				if (d.hasNext()) {
					if (!distributions.contains(instance.get(
							DistributionMongoDBObject.DOWNLOAD_URL).toString())) {
						distributions.add(instance.get(
								DistributionMongoDBObject.DOWNLOAD_URL)
								.toString());
					}
				}
			}

			for (DBObject instance : instances) {
				if (distributions.contains(instance
						.get(DistributionMongoDBObject.DOWNLOAD_URL)))
					list.add(new DistributionMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public static ArrayList<DistributionMongoDBObject> getDistributionsWithLinksFilterByDataset(
			String dataset) {

		ArrayList<DistributionMongoDBObject> list = new ArrayList<DistributionMongoDBObject>();
		ArrayList<String> distributions = new ArrayList<String>();

		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					DistributionMongoDBObject.COLLECTION_NAME);
			DBCollection collection2 = DBSuperClass.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);
			DBCursor instances = collection.find(new BasicDBObject(
					DistributionMongoDBObject.TOP_DATASET, dataset));

			for (DBObject instance : instances) {
				BasicDBObject query = new BasicDBObject(
						LinksetMongoDBObject.DISTRIBUTION_TARGET, instance.get(
								DistributionMongoDBObject.DOWNLOAD_URL)
								.toString());
				DBCursor d = collection2.find(query);

				if (d.hasNext()) {
					if (!distributions.contains(instance.get(
							DistributionMongoDBObject.DOWNLOAD_URL).toString())) {
						distributions.add(instance.get(
								DistributionMongoDBObject.DOWNLOAD_URL)
								.toString());
						// System.out.println(instance.get(DistributionMongoDBObject.DOWNLOAD_URL).toString());
					}
				}
			}

			for (DBObject instance : instances) {

				BasicDBObject query = new BasicDBObject(
						LinksetMongoDBObject.DISTRIBUTION_SOURCE, instance.get(
								DistributionMongoDBObject.DOWNLOAD_URL)
								.toString());
				DBCursor d = collection2.find(query);

				if (d.hasNext()) {
					if (!distributions.contains(instance.get(
							DistributionMongoDBObject.DOWNLOAD_URL).toString())) {
						distributions.add(instance.get(
								DistributionMongoDBObject.DOWNLOAD_URL)
								.toString());
					}
				}
			}

			for (DBObject instance : instances) {
				if (distributions.contains(instance
						.get(DistributionMongoDBObject.DOWNLOAD_URL)))
					list.add(new DistributionMongoDBObject(instance));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

}
