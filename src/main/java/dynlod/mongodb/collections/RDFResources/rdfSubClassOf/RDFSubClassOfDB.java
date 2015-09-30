package dynlod.mongodb.collections.RDFResources.rdfSubClassOf;

import java.util.Iterator;
import java.util.Set;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.collections.RDFResources.GeneralRDFResourceDB;

public class RDFSubClassOfDB extends GeneralRDFResourceDB{

	public static final String COLLECTION_NAME = "RDFSubClassOf";

	public RDFSubClassOfDB(String id) {
		super(COLLECTION_NAME, id);
		loadObject();
	}

	public RDFSubClassOfDB(int id) {
		super(COLLECTION_NAME, id);
		loadObject();
	}
	
	public RDFSubClassOfDB() {
		super();
	}

	@Override
	public void loadLocalVariables() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateLocalVariables() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insertSet(Set<String> set) {
		Iterator<String> i = set.iterator();
		RDFSubClassOfDB t = null;
		while(i.hasNext()){
			t=new RDFSubClassOfDB(i.next());
			try {
				t.updateObject(true);
			} catch (DynamicLODGeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
