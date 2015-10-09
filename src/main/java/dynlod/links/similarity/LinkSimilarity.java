package dynlod.links.similarity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import dynlod.mongodb.collections.DistributionDB;
import dynlod.mongodb.collections.LinksetDB;
import dynlod.mongodb.collections.RDFResources.GeneralRDFResourceRelationDB;
import dynlod.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import dynlod.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import dynlod.mongodb.queries.DistributionQueries;
import dynlod.mongodb.queries.PredicatesQueries;

public abstract class LinkSimilarity {
	
	public abstract double compare(Set<String> set1, Set<String> set2);
	
	GeneralRDFResourceRelationDB type;
	
	/**
	 * Update values of distribution similarities 
	 * @param distributionSource Distribution that should be compared
	 */
	public void updateLinks(DistributionDB distributionSource, GeneralRDFResourceRelationDB type ){
		this.type = type;
		
		// get all distributions except for the current one
		ArrayList<DistributionDB> distributions = new DistributionQueries().getDistributions(true);
		
//		PredicatesQueries predicates = new PredicatesQueries();
		HashSet<String> set1 ;
//			set1 = predicates.getSetOfPredicates(distribution.getDynLodID());
		if(type instanceof AllPredicatesRelationDB)
			set1 = new AllPredicatesRelationDB().getSetOfPredicates(distributionSource.getDynLodID());
		else if(type instanceof RDFTypeObjectRelationDB)
			set1 = new RDFTypeObjectRelationDB().getSetOfPredicates(distributionSource.getDynLodID());
		else if(type instanceof RDFSubClassOfRelationDB)
			set1 = new RDFSubClassOfRelationDB().getSetOfPredicates(distributionSource.getDynLodID());
//		else if(type instanceof OwlClassRelationDB)
		else
			set1 = new OwlClassRelationDB().getSetOfPredicates(distributionSource.getDynLodID());
		
		for(DistributionDB distributionTarget: distributions){
			if(distributionTarget.getDynLodID() != distributionSource.getDynLodID()){
			
			HashSet<String> set2;
			if(type instanceof AllPredicatesRelationDB)
				set2 = new AllPredicatesRelationDB().getSetOfPredicates(distributionTarget.getDynLodID());
			else if(type instanceof RDFTypeObjectRelationDB)
				set2 = new RDFTypeObjectRelationDB().getSetOfPredicates(distributionTarget.getDynLodID());
			else if(type instanceof RDFSubClassOfRelationDB)
				set2 = new RDFSubClassOfRelationDB().getSetOfPredicates(distributionTarget.getDynLodID());
//			else if(type instanceof OwlClassRelationDB)
			else
				set2 = new OwlClassRelationDB().getSetOfPredicates(distributionTarget.getDynLodID());
			
			
			double value = compare(
					set1, set2);
			
			if(value>0){
				makeLink(distributionSource, distributionTarget, value);
//				makeLink(distributionTarget, distributionSource, value);
				}
			}
		}
	}
	
	/**
	 * Update link similarity value at mongodb
	 * @param dist1 distribution 1
	 * @param dist2 distribution 2
	 * @param value similarity value
	 */
	private void makeLink(DistributionDB dist1, DistributionDB dist2, double value){
		String id = String.valueOf(dist1.getDynLodID()) + "-" + String.valueOf(dist2.getDynLodID());
		LinksetDB link = new LinksetDB(id);
//		link.setPredicateSimilarity(value);
		
		
		if(this.type instanceof AllPredicatesRelationDB)
			link.setPredicateSimilarity(value);
		else if(this.type instanceof RDFTypeObjectRelationDB)
			link.setRdfTypeSimilarity(value);
		else if(this.type instanceof RDFSubClassOfRelationDB)
			link.setRdfSubClassSimilarity(value);
//		else if(type instanceof OwlClassRelationDB)
		else
			link.setOwlClassSimilarity(value);
		

		if (link.getLinks() == 0 &&
				link.getOwlClassSimilarity() == 0 &&
				link.getRdfSubClassSimilarity()== 0 &&
				link.getRdfTypeSimilarity()== 0
				)
			return;
		
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
