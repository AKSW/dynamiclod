package dynlod.mongodb.collections.RDFResources.rdfType;

import java.util.Iterator;
import java.util.Set;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.collections.RDFResources.GeneralRDFResourceDB;

public class RDFTypeObjectDB extends GeneralRDFResourceDB{

	public static final String COLLECTION_NAME = "RDFTypeObjects";

	public RDFTypeObjectDB(String id) {
		super(COLLECTION_NAME, id);
		// TODO Auto-generated constructor stub
	}
	
	public RDFTypeObjectDB() {
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
		RDFTypeObjectDB t = null;
		while(i.hasNext()){
			t=new RDFTypeObjectDB(i.next());
			try {
				t.updateObject(true);
			} catch (DynamicLODGeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
