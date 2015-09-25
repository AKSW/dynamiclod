package dynlod.API.diagram;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import dynlod.mongodb.collections.DistributionMongoDBObject;
import dynlod.mongodb.collections.LinksetMongoDBObject;

public class DiagramData {
	
	// load all distributions
	public HashSet<Integer> distributionsID = new HashSet<Integer>();
	
	public HashMap<Integer, DistributionMongoDBObject> loadedDistributions = new HashMap<Integer, DistributionMongoDBObject>();

	public HashMap<Integer, ArrayList<LinksetMongoDBObject>> indegreeLinks = new HashMap<Integer, ArrayList<LinksetMongoDBObject>>();
	public HashMap<Integer, ArrayList<LinksetMongoDBObject>> outdegreeLinks = new HashMap<Integer, ArrayList<LinksetMongoDBObject>>();

}
