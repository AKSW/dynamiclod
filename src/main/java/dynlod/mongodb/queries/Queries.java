package dynlod.mongodb.queries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dynlod.mongodb.DBSuperClass;
import dynlod.mongodb.collections.DistributionDB;
import dynlod.mongodb.collections.LinksetDB;

public class Queries {

	public  ArrayList<String> getMongoDBObject(String collectionName,
			String field, String value) {

		ArrayList<String> list = new ArrayList<String>();
		try {
			DBCollection collection = DBSuperClass.getInstance().getCollection(
					collectionName);
			DBObject query = new BasicDBObject(field, value);
			DBCursor instances = collection.find(query);

			for (DBObject instance : instances) {
				list.add(instance.get(DBSuperClass.URI)
						.toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;

	}

}
