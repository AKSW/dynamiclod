package dynlod;

import java.util.HashMap;

import dynlod.filters.GoogleBloomFilter;
import dynlod.mongodb.collections.DistributionDB;

public class LoadedBloomFilters {

	public static HashMap<String, GoogleBloomFilter> subjectFilters = new HashMap<String, GoogleBloomFilter>(); 
	
	public static HashMap<String, GoogleBloomFilter> objectFilters = new HashMap<String, GoogleBloomFilter>(); 
	
	public static void loadSubjectFilter(DistributionDB distribution){
		if(!subjectFilters.containsKey(distribution.getUri())){
			GoogleBloomFilter f = new GoogleBloomFilter();
			f.loadFilter(distribution.getSubjectFilterPath(), distribution.getTitle());
			subjectFilters.put(distribution.getUri(), f);
		}
	}
	
	public static void loadObjectFilter(DistributionDB distribution){
		if(!objectFilters.containsKey(distribution.getUri())){
			GoogleBloomFilter f = new GoogleBloomFilter();
			f.loadFilter(distribution.getObjectFilterPath(), distribution.getTitle());
			objectFilters.put(distribution.getUri(), f);
		}
	}
	
	public static boolean querySubject(DistributionDB distribution, String query){
		boolean contains = false;
		
		loadSubjectFilter(distribution);
		
		GoogleBloomFilter f = subjectFilters.get(distribution.getUri());
		try {
			if(f.compare(query))
				return true; 
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return contains;
	}
	
	public static boolean queryObject(DistributionDB distribution, String query){
		boolean contains = false;
		
		loadObjectFilter(distribution);
		
		GoogleBloomFilter f = objectFilters.get(distribution.getUri());
		try {
			if(f.compare(query))
				return true; 
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return contains;
	}
	
}
