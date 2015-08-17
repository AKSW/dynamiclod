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
import dynlod.linksets.DistributionFQDN;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.DistributionObjectDomainsMongoDBObject;
import dynlod.mongodb.objects.DistributionSubjectDomainsMongoDBObject;

public class GetFQDNFromTriplesThread extends Thread {
	final static Logger logger = Logger
			.getLogger(GetFQDNFromTriplesThread.class);

	public boolean isSubject = false;
	
	// contains all FQDN described by distribution
	// <FQDN, <list of distribution that describes this fqdn>> 
	public ConcurrentHashMap<String, DistributionFQDN>  fqdnPerDistribution = 
			new ConcurrentHashMap<String, DistributionFQDN>();
	
	

	private String uri;
	public DistributionMongoDBObject distributionMongoDBObject = null;
	
	public HashMap<String,Integer> localFQDN = new HashMap<String,Integer>();

	private boolean doneSplittingString;

	private ConcurrentLinkedQueue<String> resourceQueue = null;
	protected ArrayList<String> resourcesToBeProcessedQueue = new  ArrayList<String>();
	public DistributionMongoDBObject distribution;
	protected ConcurrentHashMap<String, DataModelThread> listOfDataThreads = new ConcurrentHashMap<String, DataModelThread>(); 
	public ConcurrentHashMap<String, Integer> listLoadedFQDN = new ConcurrentHashMap<String, Integer>();
	


	private ConcurrentHashMap<String, Integer> countTotalFQDN = null;

	int numberOfReadedTriples = 0;

	int saveDomainsEach = 50000;

	public GetFQDNFromTriplesThread(
			ConcurrentLinkedQueue<String> resourceQueue,
			ConcurrentHashMap<String, Integer> countHashMap, String uri) {
		this.resourceQueue = resourceQueue;
		this.countTotalFQDN = countHashMap;
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
					resourcesToBeProcessedQueue.add(obj);

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
						countTotalFQDN.putIfAbsent(obj, 0);
						countTotalFQDN.replace(obj, countTotalFQDN.get(obj) + 1);
						localFQDN.put(obj, 0);

					}
					if (numberOfReadedTriples%saveDomainsEach==0){						
						makeLinks();
//						System.out.println("size: "+resourceQueue.size());
					}

				} catch (NoSuchElementException e) {
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
				
		try {
			makeLinks();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		saveDomains();
		listOfDataThreads  = new ConcurrentHashMap<String, DataModelThread>(); 
		
		logger.debug("Ending GetDomainsFromTriplesThread class.");
	}

	private boolean saveDomains() {
		logger.debug("Saving domains...");
		ObjectId id = new ObjectId();

		Iterator it = countTotalFQDN.entrySet().iterator();

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
					d2.setSubjectFQDN(d);
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
					d2.setObjectFQDN(d);
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
	
public void makeLinks() throws Exception{
		throw new Exception("You have to implement this method."); 
	}

}
