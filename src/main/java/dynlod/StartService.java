package dynlod;

import java.util.ArrayList;

import javax.servlet.http.HttpServlet;

import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.queries.Queries;
import dynlod.utils.FileUtils;

// start service properly case service was killed in the middle of streaming
public class StartService extends HttpServlet {

	public StartService() {

		new Thread(new Runnable() {

			public void run() {
				
				if (DynlodGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH == null) {
					new DynlodGeneralProperties().loadProperties();
				}
				FileUtils.checkIfFolderExists();

				ArrayList<DistributionMongoDBObject> d = new ArrayList<DistributionMongoDBObject>();

				// re-download distributions with "Downloading" status
				ArrayList<String> q = Queries.getMongoDBObject(
						DistributionMongoDBObject.COLLECTION_NAME,
						DistributionMongoDBObject.STATUS,
						DistributionMongoDBObject.STATUS_STREAMING);
				System.out
						.println("re-download distributions with \"Downloading\" status");

				for (String s : q) {
					DistributionMongoDBObject dist = new DistributionMongoDBObject(
							s);
					dist.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_STREAM);
					dist.updateObject(true);
					d.add(dist);
				}

				new Manager(d);

				// download distributions with "STATUS_WAITING_TO_DOWNLOAD"
				// status
				q = Queries.getMongoDBObject(
						DistributionMongoDBObject.COLLECTION_NAME,
						DistributionMongoDBObject.STATUS,
						DistributionMongoDBObject.STATUS_WAITING_TO_STREAM);
				System.out
						.println("download distributions with \"STATUS_WAITING_TO_DOWNLOAD\" status");

				for (String s : q) {
					DistributionMongoDBObject dist = new DistributionMongoDBObject(
							s);
					d.add(dist);
				}

				new Manager(d);
			}
		}).start();
		;

	}

}