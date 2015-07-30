package dynlod.threads;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import dynlod.filters.GoogleBloomFilter;


public class DataModelThread2 {
	
	public int setSize = 6;
	
	public int count = 0;
	public int weight = 1;
	public int weightCount = 0;
	
	public ConcurrentHashMap<Integer, ResourceInstance> urlStatus = new ConcurrentHashMap<Integer, ResourceInstance>();
	public HashMap<Integer, String> listURLToTest = new HashMap<Integer, String>();
	
	
	
	public ArrayList<String> domains = new ArrayList<String>();
	
	
	
	
	public String distributionObjectPath;
	public String subjectFilterPath;

	public String objectDistributionURI;
	public String subjectDistributionURI;

	public String objectDatasetURI;
	public String subjectDatasetURI;

	public int links = 0;
	public int ontologyLinks = 0; 
	
	public int availabilityCounter = 0 ;

	public GoogleBloomFilter filter = new GoogleBloomFilter();

	
	public DataModelThread2() {
		
	}

}
