package dynlod.linksets;

import java.util.TreeSet;

import dynlod.mongodb.objects.DistributionMongoDBObject;

public class DistributionFQDN {
	
	public int distribution;

	public DistributionMongoDBObject distributionMongoDBObject;
	
	public TreeSet<String> subjectsFQDN = new TreeSet<String>();
	
	public TreeSet<String> objectsFQDN = new TreeSet<String>();
	
	
	public boolean hasSubjectFQDN(String fqdn){
		return subjectsFQDN.contains(fqdn);
	}
	
	public boolean hasObjectFQDN(String fqdn){
		return objectsFQDN.contains(fqdn);
	}
	
	public void addSubjectsFQDN(TreeSet<String> list){
		this.subjectsFQDN = list;
	}
	
	public void addObjectsFQDN(TreeSet<String> list){
		this.objectsFQDN = list;
	}
	
	
}
