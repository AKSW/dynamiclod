package dynlod.threads;

import java.util.concurrent.atomic.AtomicBoolean;
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

	public boolean isSubject;

	// 0 for filter not loaded, 1 for loading and 2 for loaded
	public AtomicInteger filterLoaded = new AtomicInteger(0);

	public String distributionURI;
	public String datasetURI;

	public String targetDistributionURI;
	public String targetDatasetURI;
	
	public String filterPath;

	public AtomicInteger links = new AtomicInteger(0);
	public int ontologyLinks = 0;

	public GoogleBloomFilter filter = new GoogleBloomFilter();

	// flat to execute or not this model in a thread
	public boolean active = true;

	public DataModelThread(
			DistributionMongoDBObject distribution,
			DistributionMongoDBObject distributionToCompare, boolean isSubject) {

		
//		DataModelThread dataThread = new DataModelThread();
		this.isSubject = isSubject;
//		dataThread.describedFQDN = describedFQDN;

		if (!distributionToCompare.getUri().equals(distribution.getUri())) {
			// save dataThread object

			if (isSubject)
				this.filterPath = distributionToCompare
						.getObjectFilterPath();
			else
				this.filterPath = distributionToCompare
						.getSubjectFilterPath();

			
			this.targetDistributionURI = distributionToCompare
					.getUri();
			this.targetDatasetURI = distributionToCompare.getTopDataset();

			this.datasetURI = distribution.getTopDataset();
			this.distributionURI = distribution.getUri();
			// dataThread.distributionObjectPath = distribution
			// .getObjectPath();

		}

	}
	
	public void startFilter(){
		while(filterLoaded.get() == 1)
			try {
				Thread.sleep(2);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		
		if(filterLoaded.get()==2) return;
		
		try {
			
			this.filterLoaded.set(1);
			this.filter.loadFilter(filterPath);
			
			this.filterLoaded.set(2);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
