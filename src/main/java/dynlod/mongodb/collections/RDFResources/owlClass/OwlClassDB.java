package dynlod.mongodb.collections.RDFResources.owlClass;

import java.util.Iterator;
import java.util.Set;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.mongodb.collections.RDFResources.GeneralRDFResourceDB;

public class OwlClassDB extends GeneralRDFResourceDB{

	public static final String COLLECTION_NAME = "OWLClasses";

	public OwlClassDB(String id) {
		super(COLLECTION_NAME, id);
		loadObject();
	}
	
	public OwlClassDB() {
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
		OwlClassDB t = null;
		while(i.hasNext()){
			t=new OwlClassDB(i.next());
			try {
				t.updateObject(true);
			} catch (DynamicLODGeneralException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
