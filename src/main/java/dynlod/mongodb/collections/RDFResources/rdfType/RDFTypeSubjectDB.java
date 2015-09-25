package dynlod.mongodb.collections.RDFResources.rdfType;

import java.util.Iterator;
import java.util.Set;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.collections.RDFResources.GeneralRDFResourceDB;

public class RDFTypeSubjectDB extends GeneralRDFResourceDB{

	public static final String COLLECTION_NAME = "RDFTypeSubjects";

	public RDFTypeSubjectDB(String id) {
		super(COLLECTION_NAME, id);
	}
	public RDFTypeSubjectDB() {
		super();
	}

	@Override
	public void loadLocalVariables() {
	}

	@Override
	public void updateLocalVariables() {
	}

	@Override
	public void insertSet(Set<String> set) {
		Iterator<String> i = set.iterator();
		RDFTypeSubjectDB t = null;
		while(i.hasNext()){
			t=new RDFTypeSubjectDB(i.next());
			try {
				t.updateObject(true);
			} catch (DynamicLODGeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
