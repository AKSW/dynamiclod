package dynlod.links.strength;

import java.util.ArrayList;

import org.junit.Test;

import dynlod.mongodb.collections.DistributionMongoDBObject;
import dynlod.mongodb.collections.LinksetMongoDBObject;
import dynlod.mongodb.queries.DistributionQueries;
import dynlod.mongodb.queries.FQDNQueries;

public class LinkStrength {
	
	@Test
	public void Go(){
		//8996-31170
		DistributionMongoDBObject d = new DistributionMongoDBObject(49);
//		System.out.println(d.getUri());
		updateLinks(d);
//		System.out.println(new FQDNQueries().getNumberOfObjectResources(66));
	}
	
	/**
	 * Update values of distribution similarities 
	 * @param distribution Distribution that should be compared
	 */
	public void updateLinks(DistributionMongoDBObject distribution){
		
		// get all distributions except for the current one
		ArrayList<DistributionMongoDBObject> distributions = new DistributionQueries().getDistributions(true);
		
		
		for(DistributionMongoDBObject d: distributions){
			if(d.getDynLodID() != distribution.getDynLodID()){
				makeLink(distribution, d);
				makeLink(d, distribution);
				
			}
		}
	}
	/**
	 * Update link similarity value at mongodb
	 * @param dist1 distribution 1
	 * @param dist2 distribution 2
	 * @param value similarity value
	 */
	private void makeLink(DistributionMongoDBObject dist1, DistributionMongoDBObject dist2){
		String id = String.valueOf(dist1.getDynLodID()) + "-" + String.valueOf(dist2.getDynLodID());
		
//		if(dist2.getDynLodID() == 31170){ 66
			
		
		double nLinks = 0.0;
		int numberOfSourceFQDN = new FQDNQueries().getNumberOfObjectResources(dist1.getDynLodID());
		LinksetMongoDBObject link = new LinksetMongoDBObject(id);
		
					
		
		if (numberOfSourceFQDN>0){
			nLinks = 1.0*link.getLinks()/numberOfSourceFQDN;
		}
		
		link.setStrength(nLinks);
		if(link.getDatasetSource()==0)
			link.setDatasetSource(dist1.getTopDataset());
		if(link.getDatasetTarget()==0)
			link.setDatasetTarget(dist2.getTopDataset());
		
		if(link.getDistributionSource()==0)
			link.setDistributionSource(dist1.getDynLodID());
		if(link.getDistributionTarget()==0)
			link.setDistributionTarget(dist2.getDynLodID());		
		
		link.updateObject(true);
//		}
	}
	

	
	
}
