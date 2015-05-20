package dynlod.linksets;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import dynlod.exceptions.DataIDException;
import dynlod.filters.GoogleBloomFilter;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.objects.SystemPropertiesMongoDBObject;
import dynlod.mongodb.queries.DistributionQueries;
import dynlod.mongodb.queries.LinksetQueries;
import dynlod.threads.DataModelThread;
import dynlod.threads.JobThread;
import dynlod.threads.ResourceAvailability;
import dynlod.utils.Timer;

public class MakeLinksets {
	final static Logger logger = Logger.getLogger(MakeLinksets.class);

	public void updateLinksets() {

		Timer t = new Timer();
		t.startTimer();

		SystemPropertiesMongoDBObject systemProperties = new SystemPropertiesMongoDBObject();

		try {

			systemProperties.setLinksetTimeStarted(new Date());
			systemProperties.setLinksetTimeFinished(null);
			systemProperties.updateObject(true);

			logger.info("Updating linksets...");

			ArrayList<DistributionMongoDBObject> distributions = DistributionQueries
					.getDistributions();

			int distributionsAnalyzed = 0;
			int totalDistributions = distributions.size();

			for (DistributionMongoDBObject distribution : distributions) {
				distributionsAnalyzed++;

				systemProperties.setLinksetStatus(distributionsAnalyzed
						+ " of " + totalDistributions
						+ " distributions analyzed.");
				systemProperties.updateObject(true);

				if (distribution
						.getStatus()
						.equals(DistributionMongoDBObject.STATUS_WAITING_TO_CREATE_LINKSETS)
						|| distribution.getStatus().equals(
								DistributionMongoDBObject.STATUS_DONE))

					try {
						// creating list of threads to process filters
						List<DataModelThread> listOfDataThreads = new ArrayList<DataModelThread>();

						// find which filters should be opened for this
						// distribution
						ArrayList<DistributionMongoDBObject> disributionsToCompare = DistributionQueries
								.getDistributionsByAuthority((String) distribution
										.getDownloadUrl());

						// uptate status of distribution
						distribution
								.setStatus(DistributionMongoDBObject.STATUS_CREATING_LINKSETS);
						distribution.updateObject(true);

						// make some validations
						if (distribution.getObjectPath() == null
								|| distribution.getObjectPath().toString()
										.equals("")) {
							logger.error("distributionObjectPath is empty or null for "
									+ distribution.getDownloadUrl()
									+ " distribution;");
							throw new DataIDException(
									"distributionObjectPath is empty or null for "
											+ distribution.getDownloadUrl()
											+ " distribution;");
						}

						for (DistributionMongoDBObject distributionToCompare : disributionsToCompare) {
							try {

								// check if distributions had already been
								// compared
								if (!LinksetQueries.isOnLinksetList(
										distribution.getDownloadUrl(),
										distributionToCompare.getDownloadUrl()))

									if (!distributionToCompare
											.getSubjectFilterPath()
											.equals(distribution
													.getSubjectFilterPath())) {
										DataModelThread dataThread = new DataModelThread();
										// save dataThread object
										GoogleBloomFilter filter = new GoogleBloomFilter();

										try {
											filter.loadFilter(distributionToCompare
													.getSubjectFilterPath());
										} catch (Exception e) {
											e.printStackTrace();
										}
										dataThread.filter = filter;

										dataThread.subjectFilterPath = distributionToCompare
												.getSubjectFilterPath();
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

										listOfDataThreads.add(dataThread);
									}
							} catch (Exception e) {
								logger.error("Error while loading bloom filter: "
										+ e.getMessage());
								StringWriter errors = new StringWriter();
								e.printStackTrace(new PrintWriter(errors));
								System.out.println(errors.toString());
								System.out.println("distribution: "+distribution.getUri());
								System.out.println("distribution to compare: "+distributionToCompare.getUri());
						
							}

						}

						// reading object distribution file here
						BufferedReader br = new BufferedReader(new FileReader(
								distribution.getObjectPath()));

//						logger.debug("Loading objects from: "
//								+ distribution.getObjectPath()
//								+ ". This might take a time, please be patient.");

						String sCurrentLine;

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
							while ((sCurrentLine = br.readLine()) != null) {
								buffer[bufferIndex] = (sCurrentLine);
								bufferIndex++;
								int threadIndex = 0;

								// if buffer is full, start the threads!
								if (bufferIndex % bufferSize == 0) {
									Thread[] threads = new Thread[listOfDataThreads
											.size()];
									for (DataModelThread dataThread2 : listOfDataThreads) {
										threads[threadIndex] = new Thread(
												new JobThread(dataThread2,
														buffer.clone(),
														bufferSize, c));
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
							Thread[] threads = new Thread[listOfDataThreads
									.size()];
							for (DataModelThread dataThread2 : listOfDataThreads) {
								threads[threadIndex] = new Thread(
										new JobThread(dataThread2,
												buffer.clone(), bufferIndex, c));
								threads[threadIndex].start();
								threadIndex++;
							}

							// wait all threads finish and then start load
							// buffer again
							for (int d = 0; d < threads.length; d++)
								threads[d].join();

							bufferIndex = 0;

						} else {

//							logger.debug("New filters were't found!");
						}

//						logger.debug("Loaded objects from: "
//								+ distribution.getObjectPath());

						// save linksets into mongodb
						saveLinksets(listOfDataThreads, c);

						// uptate status of distribution
						distribution
								.setStatus(DistributionMongoDBObject.STATUS_DONE);
						distribution.updateObject(true);
					} catch (Exception e) {
						e.printStackTrace();
					}
				distribution.setLastTimeLinkset(String.valueOf(new Date()));
				distribution.updateObject(false);

			}

		} catch (Exception e) {
			e.printStackTrace();

			systemProperties.setLinksetTimeFinished(new Date());
			systemProperties.setLinksetStatus(e.getMessage());
			systemProperties.updateObject(true);

		}
		systemProperties.setLinksetTimeFinished(new Date());
		systemProperties.setLinksetStatus("Done");
		systemProperties.updateObject(true);

		logger.info("Time to update linksets: " + t.stopTimer() + "s");
	}

	public void saveLinksets(List<DataModelThread> dataThreads,
			ConcurrentHashMap<String, Integer> c) {

		AtomicInteger concurrentConn = new AtomicInteger(0);

		for (DataModelThread dataThread : dataThreads) {
			String mongoDBURL = dataThread.objectDistributionURI + "-2-"
					+ dataThread.subjectDistributionURI;

			System.out.println(dataThread.subjectDistributionURI);
			new ResourceAvailability(dataThread.listURLToTest, mongoDBURL, c,
					concurrentConn);

			LinksetMongoDBObject l = new LinksetMongoDBObject(mongoDBURL);

			// System.out.println(" Links working: "+positive + " "+
			// dataThread.urlStatus.size());
			// if(dataThread.urlStatus.size()>0)
			// l.setAvailability( (int)
			// ((positive/dataThread.urlStatus.size())*100));
			l.setLinks(dataThread.links);
			l.setObjectsDistributionTarget(dataThread.objectDistributionURI);
			l.setSubjectsDistributionTarget(dataThread.subjectDistributionURI);
			l.setObjectsDatasetTarget(dataThread.objectDatasetURI);
			l.setSubjectsDatasetTarget(dataThread.subjectDatasetURI);
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
