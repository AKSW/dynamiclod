package dynlod.linksets;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import dynlod.filters.GoogleBloomFilter;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.objects.SystemPropertiesMongoDBObject;
import dynlod.mongodb.queries.DistributionQueries;
import dynlod.mongodb.queries.LinksetQueries;
import dynlod.threads.DataModelThread;
import dynlod.threads.GetDomainsFromTriplesThread;
import dynlod.threads.JobThread;
import dynlod.threads.ResourceAvailability;
import dynlod.utils.Timer;

public class MakeLinksets2 {

	final static Logger logger = Logger.getLogger(MakeLinksets2.class);

	public void updateLinksets(DistributionMongoDBObject distribution,
			HashMap<String, DataModelThread> listOfDataThreads,
			ConcurrentLinkedQueue<String> buffer2, boolean isSubject) {

		Timer t = new Timer();
		t.startTimer();

		SystemPropertiesMongoDBObject systemProperties = new SystemPropertiesMongoDBObject();

		try {

			systemProperties.updateObject(true);

			logger.info("Updating linksets...");

			ArrayList<DistributionMongoDBObject> distributions = DistributionQueries
					.getDistributions();

			int distributionsAnalyzed = 0;
			int totalDistributions = distributions.size();

			systemProperties.updateObject(true);

			try {
				ArrayList<DistributionMongoDBObject> disributionsToCompare = null;
				// find which filters should be opened for this
				// distribution
				if (!isSubject)
					disributionsToCompare = DistributionQueries
							.getDistributionsByOutdegree((String) distribution
									.getDownloadUrl());
				else
					disributionsToCompare = DistributionQueries
							.getDistributionsByIndegree((String) distribution
									.getDownloadUrl());
				if(isSubject)
					logger.debug("We will compare dataset subjects " + distribution.getUri()
							+ " with " + disributionsToCompare.size() + " BF.");
				else
					logger.debug("We will compare dataset subjects " + distribution.getUri()
						+ " with " + disributionsToCompare.size() + " BF.");

				for (DistributionMongoDBObject distributionToCompare : disributionsToCompare) {
					if (!listOfDataThreads.containsKey(distributionToCompare
							.getDownloadUrl()))
						try {

							// check if distributions had already been
							// compared
							if (!LinksetQueries.isOnLinksetList(
									distribution.getDownloadUrl(),
									distributionToCompare.getDownloadUrl()))
								//
								if (!distributionToCompare.getUri().equals(
										distribution.getUri())) {
									DataModelThread dataThread = new DataModelThread();
									// save dataThread object
									GoogleBloomFilter filter = new GoogleBloomFilter();

									try {
										if(!isSubject)
											filter.loadFilter(distributionToCompare
												.getSubjectFilterPath());
										else
											filter.loadFilter(distributionToCompare
													.getObjectFilterPath());
											
									} catch (Exception e) {
										e.printStackTrace();
									}
									dataThread.filter = filter;

									if(!isSubject)
										dataThread.filterPath = distributionToCompare
											.getSubjectFilterPath();
									else
										dataThread.filterPath = distributionToCompare
										.getObjectFilterPath();										
									dataThread.subjectDistributionURI = distributionToCompare
											.getDownloadUrl();
									dataThread.subjectDatasetURI = distributionToCompare
											.getTopDataset();

									dataThread.objectDatasetURI = distribution
											.getTopDataset();
									dataThread.objectDistributionURI = distribution
											.getDownloadUrl();
									dataThread.distributionObjectPath = distribution
											.getObjectPath();

									listOfDataThreads.put(distributionToCompare
											.getDownloadUrl(), dataThread);
								}
						} catch (Exception e) {
							logger.error("Error while loading bloom filter: "
									+ e.getMessage());
							e.printStackTrace();
							// StringWriter errors = new StringWriter();
							// System.out.println(errors.toString());
							// System.out.println("distribution: "+distribution.getUri());
							// System.out.println("distribution to compare: "+distributionToCompare.getUri());

						}

				}

				// logger.debug("Loading objects from: "
				// + distribution.getObjectPath()
				// + ". This might take a time, please be patient.");

				// loading objects and creating a buffer to send to
				// threads
				int bufferSize = 500000;

				String[] buffer = new String[bufferSize];

				int bufferIndex = 0;
				ConcurrentHashMap<String, Integer> c = new ConcurrentHashMap<String, Integer>();

				if (listOfDataThreads.size() > 0) {
					logger.info("Creating liksets for distribution: "
							+ distribution.getDownloadUrl()
							+ " . We are comparing with "
							+ listOfDataThreads.size()
							+ " different bloom filters.");

					while (buffer2.size() > 0) {
						buffer[bufferIndex] = buffer2.remove();

						bufferIndex++;
						int threadIndex = 0;

						// if buffer is full, start the threads!
						if (bufferIndex % bufferSize == 0) {
							Thread[] threads = new Thread[listOfDataThreads
									.size()];
							for (DataModelThread dataThread2 : listOfDataThreads
									.values()) {
								threads[threadIndex] = new Thread(
										new JobThread(dataThread2,
												buffer.clone(), bufferSize, c));
								threads[threadIndex]
										.setName("MakeLinkSetWorker-"
												+ threadIndex);
								threads[threadIndex].start();
								threadIndex++;
							}

							// wait all threads finish and then start
							// load buffer again
							for (int d = 0; d < threads.length; d++)
								threads[d].join();

							bufferIndex = 0;

						}
					}

					int threadIndex = 0;
					// using the rest of the buffer
					Thread[] threads = new Thread[listOfDataThreads.size()];
					for (DataModelThread dataThread2 : listOfDataThreads
							.values()) {
						threads[threadIndex] = new Thread(new JobThread(
								dataThread2, buffer.clone(), bufferIndex, c));
						threads[threadIndex].start();
						threadIndex++;
					}

					// wait all threads finish and then start load
					// buffer again
					for (int d = 0; d < threads.length; d++)
						threads[d].join();

					bufferIndex = 0;

				} else {

					// logger.debug("New filters were't found!");
				}

				// logger.debug("Loaded objects from: "
				// + distribution.getObjectPath());

				// save linksets into mongodb
				saveLinksets(listOfDataThreads, c);

				// uptate status of distribution
				distribution.setStatus(DistributionMongoDBObject.STATUS_DONE);
				distribution.updateObject(true);
			} catch (Exception e) {
				e.printStackTrace();
			}
			distribution.setLastTimeLinkset(String.valueOf(new Date()));
			distribution.updateObject(false);

		} catch (Exception e) {
			e.printStackTrace();
			systemProperties.updateObject(true);
		}
		systemProperties.updateObject(true);

		logger.info("Time to update linksets: " + t.stopTimer() + "s");
	}

	public void saveLinksets(HashMap<String, DataModelThread> dataThreads,
			ConcurrentHashMap<String, Integer> c) {

		AtomicInteger concurrentConn = new AtomicInteger(0);

		for (DataModelThread dataThread : dataThreads.values()) {
			String mongoDBURL = dataThread.objectDistributionURI + "-2-"
					+ dataThread.subjectDistributionURI;

			logger.debug(dataThread.subjectDistributionURI);
			new ResourceAvailability(dataThread.listURLToTest, mongoDBURL, c,
					concurrentConn);

			LinksetMongoDBObject l = new LinksetMongoDBObject(mongoDBURL);

			// System.out.println(" Links working: "+positive + " "+
			// dataThread.urlStatus.size());
			// if(dataThread.urlStatus.size()>0)
			// l.setAvailability( (int)
			// ((positive/dataThread.urlStatus.size())*100));
			l.setLinks(dataThread.links);
			l.setDistributionSource(dataThread.objectDistributionURI);
			l.setDistributionTarget(dataThread.subjectDistributionURI);
			l.setDatasetSource(dataThread.objectDatasetURI);
			l.setDatasetTarget(dataThread.subjectDatasetURI);
			l.updateObject(true);
		}

	}

	// no parallelization method
	public void searchBufferOnFilter(GoogleBloomFilter filter, String[] lines,
			int size) throws Exception {
		BufferedReader br = null;
		ArrayList<String> links = new ArrayList<String>();

		try {
			for (int i = 0; i < size; i++) {
				if (filter.compare(lines[i])) {
					links.add(lines[i]);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();

		}

	}
}
