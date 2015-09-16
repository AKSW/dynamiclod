package dynlod.mongodb.queries;

import java.util.Iterator;
import java.util.Set;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.objects.OWLClassMongoDBObject;
import dynlod.mongodb.objects.OWLClassResourceMongoDBObject;

public class OWLClassQueries {
	/**
	 * insert a set of owl classes of a distribution
	 * @param owlClasses set of owl classes
	 * @param distributionDynLodID dynamiclod id of distribution that contains the owl classes
	 * @param topDatasetDynLodID dynamiclod id of top dataset of the distribution that contains the owl classes
	 */
	public void insertOWLClasses(Set<String> owlClasses, int distributionDynLodID, int topDatasetDynLodID){
		// save predicates
		Iterator<String> i = owlClasses.iterator();
		while(i.hasNext()){
			String owlClass = i.next();
			OWLClassMongoDBObject p = new OWLClassMongoDBObject(owlClass);
			try {
				p.updateObject(true);
				OWLClassResourceMongoDBObject pr = new OWLClassResourceMongoDBObject(p.getDynLodID()+"-"+distributionDynLodID+"-"+topDatasetDynLodID);
				pr.setDatasetID(topDatasetDynLodID);
				pr.setDistributionID(distributionDynLodID);
				pr.setClassID(p.getDynLodID());
				pr.updateObject(true);
			} catch (DynamicLODGeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}
