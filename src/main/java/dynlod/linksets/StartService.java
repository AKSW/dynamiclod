package dynlod.linksets;

import java.util.ArrayList;

import javax.servlet.http.HttpServlet;

import dynlod.Manager;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.queries.Queries;

// start service properly case service was killed in the middle of streaming
public class StartService extends HttpServlet{

	public StartService() {

		ArrayList<DistributionMongoDBObject> d = new ArrayList<DistributionMongoDBObject>();

		// re-download distributions with "Downloading" status
		ArrayList<String> q = Queries.getMongoDBObject(
				DistributionMongoDBObject.COLLECTION_NAME,
				DistributionMongoDBObject.STATUS,
				DistributionMongoDBObject.STATUS_DOWNLOADING);

		for (String s : q) {
			DistributionMongoDBObject dist = new DistributionMongoDBObject(s);
			dist.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_DOWNLOAD);
			dist.updateObject(true);
			d.add(dist);
		}

		new Manager(d);

		// download distributions with "STATUS_WAITING_TO_DOWNLOAD" status
		q = Queries.getMongoDBObject(
				DistributionMongoDBObject.COLLECTION_NAME,
				DistributionMongoDBObject.STATUS,
				DistributionMongoDBObject.STATUS_WAITING_TO_DOWNLOAD);

		for (String s : q) {
			DistributionMongoDBObject dist = new DistributionMongoDBObject(s);
			d.add(dist);
		}

		new Manager(d);

	}

}
