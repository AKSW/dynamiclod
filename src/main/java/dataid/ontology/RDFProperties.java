package dataid.ontology;

import java.util.ArrayList;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDF;

public class RDFProperties {
	
	public static final ArrayList<Property> downloadURL = new ArrayList<Property>() {
		{
			add(property(NS.DCAT_URI, "downloadURL"));
			add(property(NS.VOID_URI, "dataDump"));

		}
	};
	public static final ArrayList<Resource> Dataset =  new  ArrayList<Resource>(){{
		add(resource(NS.DATAID_URI, "Dataset"));
		add(resource(NS.VOID_URI, "Dataset"));
	}}; 
	
	public static final ArrayList<Property> distribution =  new  ArrayList<Property>(){{
		add(property(NS.DCAT_URI, "distribution"));
		add(property(NS.VOID_URI, "distribution"));
		add(property(NS.DATAID_URI, "Distribution"));
		add(property(NS.VOID_URI, "dataDump"));
	}}; 
	
	public static final Property type = RDF.type;
	
	public static final Property title = property(NS.DCT_URI, "title");
	public static final Property label = property(NS.RDFS_URI, "label");

	public static final Property subset= property(NS.VOID_URI, "subset");	 	
	public static final Property format = property(NS.DCT_URI, "format");

	public static final Property dataIDDistribution = property(NS.DATAID_URI,
			"Distribution");
	public static final Property dcatDistribution = property(NS.DCAT_URI,
			"distribution");
	
	protected static final Resource resource(String ns, String local) {
		return ResourceFactory.createResource(ns + local);
	}

	protected static final Property property(String ns, String local) {
		return ResourceFactory.createProperty(ns, local);
	}
}
