package dataid.mongodb.actions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dataid.mongodb.DataIDDB;
import dataid.mongodb.objects.DatasetMongoDBObject;
import dataid.mongodb.objects.DistributionMongoDBObject;
import dataid.mongodb.objects.DistributionObjectDomainsMongoDBObject;
import dataid.mongodb.objects.DistributionSubjectDomainsMongoDBObject;
import dataid.mongodb.objects.LinksetMongoDBObject;
import dataid.mongodb.objects.SubsetMongoDBObject;

public class Queries {
	

	// return all DataIDs file
	public static String getDistributionStatus() {
		String str = "";

		try {
			DBCollection collection = DataIDDB.getInstance().getCollection(
					DistributionMongoDBObject.COLLECTION_NAME);
			DBCursor instances = collection.find();

			for (DBObject instance : instances) {

				str = str + instance.get(DistributionMongoDBObject.DOWNLOAD_URL).toString();
				if(instance.get(DistributionMongoDBObject.SUCCESSFULLY_DOWNLOADED).toString() == "true"){
					str = str + " <span style=\"color:green\"> OK! </span>";
				}
				else{
					if(instance.get(DistributionMongoDBObject.LAST_ERROR_MSG).toString() != null)
						str = str +" <span style=\"color:red\">"+ instance.get(DistributionMongoDBObject.LAST_ERROR_MSG).toString()+"</span>";
					else
						str = str +" <span style=\"color:red\"> Error! </span>";
				}
				str = str + "<br>";
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return str;
	}	
	
	

	public void getDatasetsLinksets() {
		try {

			DBCollection collection = DataIDDB.getInstance().getCollection(
					LinksetMongoDBObject.COLLECTION_NAME);

			CommandResult commandResult = DataIDDB
					.getInstance()
					.command(
							"db.Linkset.group({key: { 'objectsDatasetTarget': 1, 'subjectsDatasetTarget': 1 },"
									+ "reduce: function( curr, result ) { "
									+ "result.total += curr.links; "
									+ " },  "
									+ " initial: { total : 0} " + "})");

			// System.out.println(commandResult);

			// build the $projection operation
			DBObject fields = new BasicDBObject("objectsDatasetTarget", 1);
			fields.put("subjectsDatasetTarget", 1);
			DBObject project = new BasicDBObject("$project", fields);

			// Now the $group operation
			DBObject groupFields = new BasicDBObject("_id", new BasicDBObject(
					"objectsDatasetTarget", "$objectsDatasetTarget").append(
					"subjectsDatasetTarget", "$subjectsDatasetTarget"));
			DBObject group = new BasicDBObject("$group", groupFields);

			// Finally the $sort operation
			DBObject sort = new BasicDBObject("$sort", new BasicDBObject(
					"objectsDatasetTarget", 1).append("subjectsDatasetTarget",
					1));

			// run aggregation
			List<DBObject> pipeline = Arrays.asList(project, group, sort);
			AggregationOutput output = collection.aggregate(pipeline);

			for (DBObject result : output.results()) {
				System.out.println(result);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
