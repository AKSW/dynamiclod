package dynlod;

import java.util.ArrayList;

import javax.servlet.http.HttpServlet;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import dynlod.mongodb.DBSuperClass;
import dynlod.mongodb.IndexesCreator;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.DistributionObjectDomainsMongoDBObject;
import dynlod.mongodb.objects.DistributionSubjectDomainsMongoDBObject;
import dynlod.mongodb.queries.Queries;
import dynlod.utils.FileUtils;

// start service properly case service was killed in the middle of streaming
public class StartService extends HttpServlet {

	final static Logger logger = Logger.getLogger(StartService.class);

	public StartService() {

		new Thread(new Runnable() {

			public void run() {

				try {
					BasicConfigurator.configure();

					DynlodGeneralProperties properties = new DynlodGeneralProperties();

					if (DynlodGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH == null) {
						properties.loadProperties();
					}

					FileUtils.checkIfFolderExists();

					// creating indexes
					new IndexesCreator().createIndexes();

					ArrayList<DistributionMongoDBObject> d = new ArrayList<DistributionMongoDBObject>();

					if (properties.RESUME) {
						// re-download distributions with "Downloading" status
						ArrayList<String> q = Queries.getMongoDBObject(
								DistributionMongoDBObject.COLLECTION_NAME,
								DistributionMongoDBObject.STATUS,
								DistributionMongoDBObject.STATUS_STREAMING);
						logger.debug("re-download distributions with \""
								+ DistributionMongoDBObject.STATUS_STREAMING
								+ "\" status");

						for (String s : q) {
							DistributionMongoDBObject dist = new DistributionMongoDBObject(
									s);
							dist.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_STREAM);
							dist.updateObject(true);
							d.add(dist);
						}

						// new

						// download distributions with
						// "STATUS_WAITING_TO_STREAM"
						// status
						q = Queries
								.getMongoDBObject(
										DistributionMongoDBObject.COLLECTION_NAME,
										DistributionMongoDBObject.STATUS,
										DistributionMongoDBObject.STATUS_WAITING_TO_STREAM);
						logger.debug("download distributions with \""
								+ DistributionMongoDBObject.STATUS_WAITING_TO_STREAM
								+ "\" status");

						for (String s : q) {
							DistributionMongoDBObject dist = new DistributionMongoDBObject(
									s);
							dist.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_STREAM);
							dist.updateObject(true);
							d.add(dist);
						}

						// download distributions with "ERROR"
						// status
						q = Queries.getMongoDBObject(
								DistributionMongoDBObject.COLLECTION_NAME,
								DistributionMongoDBObject.STATUS,
								DistributionMongoDBObject.STATUS_ERROR);
						logger.debug("download distributions with \""
								+ DistributionMongoDBObject.STATUS_WAITING_TO_STREAM
								+ "\" status");

						for (String s : q) {
							DistributionMongoDBObject dist = new DistributionMongoDBObject(
									s);
							dist.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_STREAM);
							dist.updateObject(true);
							d.add(dist);
						}

						new Manager(d);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();
		;

	}

}
