package dynlod.threads;

import java.util.concurrent.atomic.AtomicInteger;

import dynlod.filters.GoogleBloomFilter;
import dynlod.mongodb.objects.DistributionMongoDBObject;

public class DataModelThread {

	// true if the source distribution is the subject column
	//
	// sourceColumnIsSubject = true
	//
	// Target Source Target
	// BF dist. BF
	// ____ __________ ____
	// | o| <- | s| p| o| |s |
	// | o| <- | s| p| o| |s |
	// | o| <- | s| p| o| |s |
	// | o| <- | s| p| o| |s |
	// | o| <- | s| p| o| |s |
	//
	// sourceColumnIsSubject = false
	//
	// Target Source Target
	// BF dist. BF
	// ____ __________ ____
	// | o| | s| p| o| -> |s |
	// | o| | s| p| o| -> |s |
	// | o| | s| p| o| -> |s |
	// | o| | s| p| o| -> |s |
	//
	//

	public boolean sourceColumnIsSubject;

	// represents what FQDN are described by this filter
//	public HashMap<String, Integer> describedFQDN = new HashMap<String, Integer>();

	// public ConcurrentHashMap<Integer, ResourceInstance> urlStatus = new
	// ConcurrentHashMap<Integer, ResourceInstance>();
	// public HashMap<Integer, String> listURLToTest = new HashMap<Integer,
	// String>();
	// public String distributionObjectPath;
	// public int availabilityCounter = 0 ;

	// path for the filter
	public String filterPath;

	public String distributionURI;
	public String datasetURI;

	public String targetDistributionURI;
	public String targetDatasetURI;

	public AtomicInteger links = new AtomicInteger(0);
	public int ontologyLinks = 0;

	public GoogleBloomFilter filter = new GoogleBloomFilter();

	// flat to execute or not this model in a thread
	public boolean active = true;

	public DataModelThread(
			DistributionMongoDBObject distribution,
			DistributionMongoDBObject distributionToCompare, boolean sourceColumnIsSubject) {

		
//		DataModelThread dataThread = new DataModelThread();
		this.sourceColumnIsSubject = sourceColumnIsSubject;
//		dataThread.describedFQDN = describedFQDN;

		if (!distributionToCompare.getUri().equals(distribution.getUri())) {
			// save dataThread object
			GoogleBloomFilter filter = new GoogleBloomFilter();

			try {
				if (!sourceColumnIsSubject)
					filter.loadFilter(distributionToCompare
							.getSubjectFilterPath());
				else
					filter.loadFilter(distributionToCompare
							.getObjectFilterPath());

			} catch (Exception e) {
				e.printStackTrace();
			}
			this.filter = filter;

			
			this.targetDistributionURI = distributionToCompare
					.getUri();
			this.targetDatasetURI = distributionToCompare.getTopDataset();

			this.datasetURI = distribution.getTopDataset();
			this.distributionURI = distribution.getUri();
			// dataThread.distributionObjectPath = distribution
			// .getObjectPath();

		}

	}

}
