package dataid.API;

import java.util.ArrayList;

import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import dataid.mongodb.objects.DistributionMongoDBObject;
import dataid.mongodb.objects.LinksetMongoDBObject;
import dataid.mongodb.queries.LinksetQueries;
import dataid.ontology.Dataset;
import dataid.ontology.NS;

public class APIRetrieve extends API {

	public String URI;
	public Model outModel = null;

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	public APIRetrieve(String URI) {

		this.URI = URI;

		outModelInit();

		retrieveByDistribution();

		printModel();

	}

	private void outModelInit() {
		outModel = ModelFactory.createDefaultModel();
		outModel.setNsPrefix("rdfs", NS.RDFS_URI);
		outModel.setNsPrefix("dcat", NS.DCAT_URI);
		outModel.setNsPrefix("void", NS.VOID_URI);
		outModel.setNsPrefix("sd", NS.SD_URI);
		outModel.setNsPrefix("prov", NS.PROV_URI);
		outModel.setNsPrefix("dct", NS.DCT_URI);
		outModel.setNsPrefix("xsd", NS.XSD_URI);
		outModel.setNsPrefix("foaf", NS.FOAF_URI);
	}

	private void printModel() {
		outModel.write(System.out, "TURTLE");
	}

	public void retrieveByDistribution() {

		// get indegree and outdegree for a distribution
		ArrayList<LinksetMongoDBObject> in = LinksetQueries
				.getLinksetsInDegreeByDistribution(URI);
		ArrayList<LinksetMongoDBObject> out = LinksetQueries
				.getLinksetsOutDegreeByDistribution(URI);

		// add choosen distribution to jena
		addDistributionToModel(new DistributionMongoDBObject(URI));

		// add linksets to jena model
		for (LinksetMongoDBObject linkset : in) {
			DistributionMongoDBObject distribution = new DistributionMongoDBObject(
					linkset.getObjectsDistributionTarget());
			addDistributionToModel(distribution);
			addLinksetToModel(linkset);
		}
		// add linksets to jena model
		for (LinksetMongoDBObject linkset : out) {
			DistributionMongoDBObject distribution = new DistributionMongoDBObject(
					linkset.getSubjectsDistributionTarget());
			addDistributionToModel(distribution);
			addLinksetToModel(linkset);
		}

	}

	private void addDistributionToModel(DistributionMongoDBObject distribution) {
		// add distribution to jena model
		Resource r = outModel.createResource(distribution.getDownloadUrl());
		r.addProperty(Dataset.type,
				ResourceFactory.createResource(NS.VOID_URI + "Dataset"));
		
		String name;
		
		if(distribution.getTitle()==null)
			name = distribution.getUri();
		else name = distribution.getTitle();

		r.addProperty(Dataset.title, name);
	}

	private void addLinksetToModel(LinksetMongoDBObject linkset) {
		// add linksets
			Resource r = outModel.createResource(linkset.getUri());
			r.addProperty(Dataset.type,
					ResourceFactory.createResource(NS.VOID_URI + "Linkset"));
			r.addProperty(ResourceFactory.createProperty(NS.VOID_URI
					+ "objectsTarget"), ResourceFactory.createResource(linkset
					.getSubjectsDistributionTarget().toString()));
			r.addProperty(ResourceFactory.createProperty(NS.VOID_URI
					+ "subjectsTarget"), ResourceFactory.createResource(linkset
					.getObjectsDistributionTarget().toString()));

			r.addProperty(ResourceFactory.createProperty(NS.VOID_URI
					+ "triples"), ResourceFactory.createPlainLiteral(String
					.valueOf(linkset.getLinks())));
	}

}
