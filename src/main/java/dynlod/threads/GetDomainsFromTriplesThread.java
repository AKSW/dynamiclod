package dynlod.threads;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.linksets.MakeLinksets2;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.DistributionObjectDomainsMongoDBObject;
import dynlod.mongodb.objects.DistributionSubjectDomainsMongoDBObject;

public class GetDomainsFromTriplesThread extends Thread {

	final static Logger logger = Logger
			.getLogger(GetDomainsFromTriplesThread.class);

	public boolean isSubject = false;

	private String uri;
	public DistributionMongoDBObject distributionMongoDBObject = null;

	private boolean doneSplittingString;

	private ConcurrentLinkedQueue<String> resourceQueue = null;
	private ArrayList<String> resourecesToBeProcessedQueue = new  ArrayList<String>();
	DistributionMongoDBObject distribution;
	ConcurrentHashMap<String, DataModelThread> listOfDataThreads = new ConcurrentHashMap<String, DataModelThread>(); 
	public ConcurrentHashMap<String, Integer> listLoadedFQDN = new ConcurrentHashMap<String, Integer>();
	


	private ConcurrentHashMap<String, Integer> countHashMap = null;

	int numberOfReadedTriples = 0;

	int saveDomainsEach = 20000;

	public GetDomainsFromTriplesThread(
			ConcurrentLinkedQueue<String> resourceQueue,
			ConcurrentHashMap<String, Integer> countHashMap, String uri) {
		this.resourceQueue = resourceQueue;
		this.countHashMap = countHashMap;
		this.uri = uri;
		this.distribution = new DistributionMongoDBObject(uri);

	}

	public boolean isDoneSplittingString() {
		return doneSplittingString;
	}

	public void setDoneSplittingString(boolean doneSplittingString) {
		this.doneSplittingString = doneSplittingString;
	}

	public synchronized void run() {

		logger.debug("Starting GetDomainsFromTriplesThread class.");

		String obj = "";
		while (!doneSplittingString) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			while (resourceQueue.size() > 0) {
				numberOfReadedTriples++;
				try {
					obj = resourceQueue.remove();
					resourecesToBeProcessedQueue.add(obj);

					if (obj.startsWith("<"))
						obj = obj.substring(1, obj.length() - 1);
					else
						obj = obj.substring(0, obj.length() - 1);

					String[] ar = obj.split("/");
					if (ar.length > 3)
						obj = ar[0] + "//" + ar[2] + "/" + ar[3] + "/";
					else if (ar.length > 2)
						obj = ar[0] + "//" + ar[2] + "/";
					else {
						obj = "";
					}

					if (!obj.equals("")) {
						countHashMap.putIfAbsent(obj, 1);
						countHashMap.replace(obj, countHashMap.get(obj) + 1);

					}
					if (numberOfReadedTriples%saveDomainsEach==0){
//						saveDomains();
//						new MakeLinksets2().start(distribution, listOfDataThreads, 
//								resourecesToBeProcessedQueue, isSubject, countHashMap);
						MakeLinksets2 m = new MakeLinksets2(distribution, listOfDataThreads, 
								(ArrayList<String>) resourecesToBeProcessedQueue.clone(), isSubject, countHashMap, listLoadedFQDN);
						resourecesToBeProcessedQueue = new ArrayList<String>();
						m.start();
					}

				} catch (NoSuchElementException e) {
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		
		ConcurrentLinkedQueue<String> clone = new ConcurrentLinkedQueue<String>();
		MakeLinksets2 m = new MakeLinksets2(distribution, listOfDataThreads, 
				(ArrayList<String>) resourecesToBeProcessedQueue.clone(), isSubject, countHashMap, listLoadedFQDN);
		m.start();
		try {
			m.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		saveDomains();
		
		logger.debug("Ending GetDomainsFromTriplesThread class.");
	}

	private boolean saveDomains() {
		logger.debug("Saving domains...");
		ObjectId id = new ObjectId();

		Iterator it = countHashMap.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String d = (String) pair.getKey();
			int count = (Integer) pair.getValue();
			// distributionMongoDBObj.addAuthorityObjects(d);

			if (count > 50) {
				id = new ObjectId();
				if (isSubject) {
					DistributionSubjectDomainsMongoDBObject d2 = new DistributionSubjectDomainsMongoDBObject(id.get()
							.toString());
					d2.setSubjectDomain(d);
					d2.setDistributionURI(uri);
					
					try {
						d2.updateObject(true);

					} catch (DynamicLODGeneralException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {

					DistributionObjectDomainsMongoDBObject d2 = null;
					d2 = new DistributionObjectDomainsMongoDBObject(id.get()
							.toString());
					d2.setObjectDomain(d);
					d2.setDistributionURI(uri);

					try {
						d2.updateObject(true);
					} catch (DynamicLODGeneralException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

			}
		}
		
		return true;
	}

}
