package dynlod;

import java.util.ArrayList;

import javax.servlet.http.HttpServlet;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import dynlod.mongodb.IndexesCreator;
import dynlod.mongodb.collections.DistributionDB;
import dynlod.mongodb.queries.Queries;
import dynlod.utils.FileUtils;

/**
 * start service properly. This class checks whether the application have
 * to keep streaming files (means that app was killed before finish their work),
 * and whether have to create MongoDB indexes
 * @author ciro
 *
 */
public class StartService extends HttpServlet {

	private static final long serialVersionUID = 9131804335500741880L;
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

					ArrayList<DistributionDB> distributions = new ArrayList<DistributionDB>();

					if (DynlodGeneralProperties.RESUME) {
						
						// re-download distributions with "Downloading" status
						ArrayList<String> q = new Queries().getMongoDBObject(
								DistributionDB.COLLECTION_NAME,
								DistributionDB.STATUS,
								DistributionDB.STATUS_STREAMING);
						logger.debug("re-download distributions with \""
								+ DistributionDB.STATUS_STREAMING
								+ "\" status");

						for (String s : q) {
							DistributionDB dist = new DistributionDB(
									s);
							dist.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);
							dist.updateObject(true);
//							distributions.add(dist);
						}

						// new

						// download distributions with
						// "STATUS_WAITING_TO_STREAM"
						// status
						q = new Queries()
								.getMongoDBObject(
										DistributionDB.COLLECTION_NAME,
										DistributionDB.STATUS,
										DistributionDB.STATUS_WAITING_TO_STREAM);
						logger.debug("download distributions with \""
								+ DistributionDB.STATUS_WAITING_TO_STREAM
								+ "\" status");

						for (String s : q) {
							DistributionDB dist = new DistributionDB(
									s);
							dist.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);
							dist.updateObject(true);
							distributions.add(dist);
						}

					}
					
					if(DynlodGeneralProperties.RESUME_ERRORS){
						// download distributions with "ERROR"
						// status
						ArrayList<String> q = new Queries().getMongoDBObject(
								DistributionDB.COLLECTION_NAME,
								DistributionDB.STATUS,
								DistributionDB.STATUS_ERROR);
						logger.debug("download distributions with \""
								+ DistributionDB.STATUS_WAITING_TO_STREAM
								+ "\" status");

						for (String s : q) {
							DistributionDB dist = new DistributionDB(
									s);
							dist.setStatus(DistributionDB.STATUS_WAITING_TO_STREAM);
							dist.updateObject(true);
							distributions.add(dist);
						}
					}

					new Manager(distributions);

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();
		;

	}

}
