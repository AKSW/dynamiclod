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

public class MakeLinksets extends GetFQDNFromTriplesThread {

	public MakeLinksets(ConcurrentLinkedQueue<String> resourceQueue,
			ConcurrentHashMap<String, Integer> countHashMap, String uri) {
		super(resourceQueue, countHashMap, uri);
	}

	final static Logger logger = Logger.getLogger(MakeLinksets.class);

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
			Thread t = new Thread(new Runnable() {
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
					// list  (so we don't have to query again)
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
								.containsKey(distributionToCompare.getDynLodID()))
							try {

								// check if distributions had already been
								// compared
								if (!distributionToCompare.getUri().equals(
										distribution.getUri())) {
									DataModelThread dataThread = new DataModelThread(
											distribution,
											distributionToCompare, 
											fqdnPerDistribution.get(distributionToCompare.getDynLodID()),
											isSubject);
									if (dataThread.datasetID != 0) {
										listOfDataThreads.putIfAbsent(
												distributionToCompare.getDynLodID(),
												dataThread);
										dataThread = listOfDataThreads.get(distributionToCompare.getDynLodID());
										dataThread.startFilter();
										
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
									boolean keepTrying = true;
									while(keepTrying){
										try{
											if(! (dFqdn.distribution == distribution.getDynLodID())){
												listOfDataThreads.get(dFqdn.distribution).active = true;
											}
											keepTrying = false;
										}
										catch (Exception e){
//											e.printStackTrace();
											try {
												Thread.sleep(1);
											} catch (InterruptedException e1) {
												// TODO Auto-generated catch block
											}
										}
									}
								}
							}
						} else {
							for (String fqdn : localFQDNCopy.keySet()) {
								if (dFqdn.hasSubjectFQDN(fqdn)) {
									
									boolean keepTrying = true;
									while(keepTrying){
										try{
											if(!(dFqdn.distribution == distribution.getDynLodID()))
												listOfDataThreads.get(dFqdn.distribution).active = true;
											
											keepTrying = false;
										}
										catch (Exception e){
//											e.printStackTrace();											
											try {
												Thread.sleep(1);
											} catch (InterruptedException e1) {
												// TODO Auto-generated catch block
											}
										}
									}
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
												+ threadIndex + dataThread2.targetDistributionID);
								threads[threadIndex].start();
								threadIndex++;
							}
						}

						// wait all threads finish 
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
						
						LinksetMongoDBObject l; 

						String mongoDBURL;

						

						// System.out.println(" Links working: "+positive + "
						
						if(isSubject){
							 mongoDBURL = dataThread.targetDistributionID+ "-" + dataThread.distributionID;
							 l = new LinksetMongoDBObject(
										mongoDBURL);
							l.setDistributionSource(dataThread.targetDistributionID);
							l.setDistributionTarget(dataThread.distributionID);
							l.setDatasetSource(dataThread.targetDatasetID);
							l.setDatasetTarget(dataThread.datasetID);
							
						}
						else{
							 mongoDBURL = dataThread.distributionID + "-2-"
										+ dataThread.targetDistributionID;
							 l = new LinksetMongoDBObject(
										mongoDBURL);
						
						l.setDistributionSource(dataThread.distributionID);
						l.setDistributionTarget(dataThread.targetDistributionID);
						l.setDatasetSource(dataThread.datasetID);
						l.setDatasetTarget(dataThread.targetDatasetID);
						}
						l.setLinks(dataThread.links.get());
						l.setInvalidLinks(dataThread.invalidLinks.get());
						if(l.getLinks()>0 || l.getInvalidLinks()>0)
							l.updateObject(true); 
					}

				}
			});
			threadNumber++;
			t.setName("MakingLinksets:"+(threadNumber)+":"+distribution.getUri());
			listOfThreads.putIfAbsent("MakingLinksets:"+(threadNumber)+":"+distribution.getUri(), t);
			t.start();
			

		} catch (Exception e) {

		}
	}
}
