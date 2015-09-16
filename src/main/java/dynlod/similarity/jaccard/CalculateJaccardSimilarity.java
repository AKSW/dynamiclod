package dynlod.similarity.jaccard;

import java.util.ArrayList;
import java.util.HashSet;

import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.queries.DistributionQueries;
import dynlod.mongodb.queries.PredicatesQueries;

public class CalculateJaccardSimilarity {
	
	/**
	 * Update values of distribution similarities 
	 * @param distribution Distribution that should be compared
	 */
	public void updateLinks(DistributionMongoDBObject distribution){
		
		// get all distributions except for the current one
		ArrayList<DistributionMongoDBObject> distributions = new DistributionQueries().getDistributions(true);
		
		PredicatesQueries predicates = new PredicatesQueries();
		
		HashSet<String> set1 = predicates.getSetOfPredicates(distribution.getDynLodID());
		
		for(DistributionMongoDBObject d: distributions){
			if(d.getDynLodID() != distribution.getDynLodID()){
			
			HashSet<String> set2 = predicates.getSetOfPredicates(d.getDynLodID());
			
			double value = new JaccardSimilarity().compare(
					set1, set2);
			
			if(value>0){
				makeLink(distribution, d, value);
				makeLink(d, distribution, value);
				}
			}
		}
	}
	
	private void makeLink(DistributionMongoDBObject dist1, DistributionMongoDBObject dist2, double value){
		String id = String.valueOf(dist1.getDynLodID()) + "-2-" + String.valueOf(dist2.getDynLodID());
		LinksetMongoDBObject link = new LinksetMongoDBObject(id);
		link.setJaccardSimilarity(value);
		if(link.getDatasetSource()==0)
			link.setDatasetSource(dist1.getTopDataset());
		if(link.getDatasetTarget()==0)
			link.setDatasetTarget(dist2.getTopDataset());
		
		if(link.getDistributionSource()==0)
			link.setDistributionSource(dist1.getDynLodID());
		if(link.getDistributionTarget()==0)
			link.setDistributionTarget(dist2.getDynLodID());		
		
		link.updateObject(true);
	}
}
