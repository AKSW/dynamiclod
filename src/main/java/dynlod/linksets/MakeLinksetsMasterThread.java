package dynlod.linksets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import dynlod.mongodb.collections.DistributionDB;
import dynlod.mongodb.collections.LinksetDB;
import dynlod.mongodb.queries.DistributionQueries;
import dynlod.threads.DataModelThread;
import dynlod.threads.GetFQDNFromTriplesThread;
import dynlod.threads.JobThread;

public class MakeLinksetsMasterThread extends GetFQDNFromTriplesThread {

	/**
	 * Create linksets for a distribution
	 * @param resourceQueue string queue of objects or subjects resources
	 * @param uri of the distribution (usually the distribution URL)
	 */
	public MakeLinksetsMasterThread(ConcurrentLinkedQueue<String> resourceQueue,
			String uri) {
		super(resourceQueue, uri);
	}

	final static Logger logger = Logger.getLogger(MakeLinksetsMasterThread.class);

	ArrayList<DistributionDB> distributionsToCompare;
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
						distributionsToCompare = new DistributionQueries()
								.getDistributionsByOutdegree(fqdnToSearch,
										fqdnPerDistribution);
					else
						distributionsToCompare = new DistributionQueries()
								.getDistributionsByIndegree(fqdnToSearch,
										fqdnPerDistribution);
					

					for (DistributionDB distributionToCompare : distributionsToCompare) {
						if (!listOfWorkerThreads
								.containsKey(distributionToCompare.getDynLodID()))
							try {

								// check if distributions had already been
								// compared
								if (!distributionToCompare.getUri().equals(
										distribution.getUri())) {
									DataModelThread workerThread = new DataModelThread(
											distribution,
											distributionToCompare, 
											fqdnPerDistribution.get(distributionToCompare.getDynLodID()),
											isSubject);
									if (workerThread.datasetID != 0) {
										listOfWorkerThreads.putIfAbsent(
												distributionToCompare.getDynLodID(),
												workerThread);
										workerThread = listOfWorkerThreads.get(distributionToCompare.getDynLodID());
										workerThread.startFilter();
										
									}
								}
							} catch (Exception e) {
								logger.error("Error: "
										+ e.getMessage());
//								System.out.println(distributionToCompare);
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
												listOfWorkerThreads.get(dFqdn.distribution).active = true;
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
												listOfWorkerThreads.get(dFqdn.distribution).active = true;
											
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

					if (listOfWorkerThreads.size() > 0) {

						int threadIndex = 0;

						Thread[] threads = new Thread[listOfWorkerThreads.size()];
						for (DataModelThread dataThread2 : listOfWorkerThreads
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
					for (DataModelThread dataThread : listOfWorkerThreads
							.values()) {

						dataThread.active = false;
						
						LinksetDB l; 

						String mongoDBURL;

						

						// System.out.println(" Links working: "+positive + "
						
						if(isSubject){
							 mongoDBURL = dataThread.targetDistributionID+ "-" + dataThread.distributionID;
							 l = new LinksetDB(
										mongoDBURL);
							l.setDistributionSource(dataThread.targetDistributionID);
							l.setDistributionTarget(dataThread.distributionID);
							l.setDatasetSource(dataThread.targetDatasetID);
							l.setDatasetTarget(dataThread.datasetID);
							
						}
						else{
							 mongoDBURL = dataThread.distributionID + "-"
										+ dataThread.targetDistributionID;
							 l = new LinksetDB(
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
