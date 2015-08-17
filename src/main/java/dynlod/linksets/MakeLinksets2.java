package dynlod.linksets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.queries.DistributionQueries;
import dynlod.threads.DataModelThread;
import dynlod.threads.GetFQDNFromTriplesThread;
import dynlod.threads.JobThread;

public class MakeLinksets2 extends GetFQDNFromTriplesThread {

	public MakeLinksets2(ConcurrentLinkedQueue<String> resourceQueue,
			ConcurrentHashMap<String, Integer> countHashMap, String uri) {
		super(resourceQueue, countHashMap, uri);
	}

	final static Logger logger = Logger.getLogger(MakeLinksets2.class);

	ArrayList<DistributionMongoDBObject> disributionsToCompare;
	ArrayList<String> resourcesToBeProcessedQueueCopy;

	public HashMap<String, Integer> localFQDNCopy = new HashMap<String, Integer>();

	@Override
	public void makeLinks() {

		localFQDNCopy = (HashMap<String, Integer>) localFQDN.clone();
		resourcesToBeProcessedQueueCopy = (ArrayList<String>) resourcesToBeProcessedQueue
				.clone();
		localFQDN = new HashMap<String, Integer>();
		resourcesToBeProcessedQueue = new ArrayList<String>();

		try {
			new Thread(new Runnable() {
				public void run() {

					ArrayList<String> fqdnToSearch = new ArrayList<String>();

					// create a list of FQDN which should be fetched from
					// database
					// and add the loaded FQDN to a global map
					for (String fqdn : localFQDNCopy.keySet()) {
						if (!listLoadedFQDN.containsKey(fqdn)) {
							fqdnToSearch.add(fqdn);
							listLoadedFQDN.putIfAbsent(fqdn, 0);
						}
					}

					// get which distributions describe which FQDN and save in a
					// list
					// (so we don't have to query again)
					if (!isSubject)
						disributionsToCompare = new DistributionQueries()
								.getDistributionsByOutdegree(fqdnToSearch,
										fqdnPerDistribution);
					else
						disributionsToCompare = new DistributionQueries()
								.getDistributionsByIndegree(fqdnToSearch,
										fqdnPerDistribution);

					for (DistributionMongoDBObject distributionToCompare : disributionsToCompare) {
						if (!listOfDataThreads
								.containsKey(distributionToCompare.getUri()))
							try {

								// check if distributions had already been
								// compared
								if (!distributionToCompare.getUri().equals(
										distribution.getUri())) {
									DataModelThread dataThread = new DataModelThread(
											distribution,
											distributionToCompare, isSubject);
									if (dataThread.datasetURI != null) {
										listOfDataThreads.put(
												distributionToCompare.getUri(),
												dataThread);
									}
								}
							} catch (Exception e) {
								logger.error("Error while loading bloom filter: "
										+ e.getMessage());
								e.printStackTrace();
							}
					}

					for (DistributionFQDN dFqdn : fqdnPerDistribution.values()) {
						// check whether fqdn is in the subject list
						if (isSubject) {
							for (String fqdn : localFQDNCopy.keySet()) {
								if (dFqdn.hasObjectFQDN(fqdn)) {
									listOfDataThreads.get(dFqdn.distribution).active = true;
								}
							}
						} else {
							for (String fqdn : localFQDNCopy.keySet()) {
								if (dFqdn.hasSubjectFQDN(fqdn)) {
									listOfDataThreads.get(dFqdn.distribution).active = true;
								}
							}
						}

					}

					int bufferSize = resourcesToBeProcessedQueueCopy.size();

					String[] buffer = new String[bufferSize];

					if (listOfDataThreads.size() > 0) {

						int threadIndex = 0;

						Thread[] threads = new Thread[listOfDataThreads.size()];
						for (DataModelThread dataThread2 : listOfDataThreads
								.values()) {
							if (dataThread2.active) {
								threads[threadIndex] = new Thread(
										new JobThread(
												dataThread2,
												(ArrayList<String>) resourcesToBeProcessedQueueCopy
														.clone()));
								threads[threadIndex]
										.setName("MakeLinkSetWorker-"
												+ threadIndex);
								threads[threadIndex].start();
								threadIndex++;
							}
						}

						// wait all threads finish and then start
						// load buffer again
						for (int d = 0; d < threads.length; d++)
							try {
								threads[d].join();
							} catch (InterruptedException e) {
//								e.printStackTrace();
							}
							catch (Exception e) {
								// TODO: handle exception
							}

					}

					// save linksets into mongodb
					for (DataModelThread dataThread : listOfDataThreads
							.values()) {

						dataThread.active = false;

						String mongoDBURL = dataThread.distributionURI + "-2-"
								+ dataThread.targetDistributionURI;

						LinksetMongoDBObject l = new LinksetMongoDBObject(
								mongoDBURL);

						// System.out.println(" Links working: "+positive + "
						l.setLinks(dataThread.links.get());
						l.setDistributionSource(dataThread.distributionURI);
						l.setDistributionTarget(dataThread.targetDistributionURI);
						l.setDatasetSource(dataThread.datasetURI);
						l.setDatasetTarget(dataThread.targetDatasetURI);
						l.updateObject(true);
					}

				}
			}).start();

		} catch (Exception e) {

		}
	}
}
