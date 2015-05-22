package dynlod.API;

import java.util.ArrayList;

import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.queries.DatasetQueries;
import dynlod.mongodb.queries.LinksetQueries;
import dynlod.ontology.NS;
import dynlod.ontology.RDFProperties;

public class APIRetrieve extends API {

	public Model outModel = null;

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}
//	public APIRetrieve() {
//		// TODO Auto-generated constructor stub
//	}

	public APIRetrieve(String URI) {

		outModelInit();
		DatasetMongoDBObject d = new DatasetMongoDBObject(URI, true);
		getDatasetChildren(d);
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

	public void retrieveByDistribution(String distributionURI) {
		// get indegree and outdegree for a distribution
		ArrayList<LinksetMongoDBObject> in = LinksetQueries
				.getLinksetsInDegreeByDistribution(distributionURI);
		ArrayList<LinksetMongoDBObject> out = LinksetQueries
				.getLinksetsOutDegreeByDistribution(distributionURI);

		// add choosen distribution to jena
//		addDistributionToModel(new DistributionMongoDBObject(distributionURI));

		// add linksets to jena model
		for (LinksetMongoDBObject linkset : in) {
			DistributionMongoDBObject distributionSubject = new DistributionMongoDBObject(
					linkset.getSubjectsDistributionTarget());

			DistributionMongoDBObject distributionObject = new DistributionMongoDBObject(
					linkset.getObjectsDistributionTarget());

			for (String d1 : distributionSubject.getDefaultDatasets()) {
				for (String d2 : distributionObject.getDefaultDatasets()) {
					addLinksetToModel(d2,d1,linkset.getLinks());
				}
			}
		}
		// add linksets to jena model
		for (LinksetMongoDBObject linkset : out) {
			DistributionMongoDBObject distributionSubject = new DistributionMongoDBObject(
					linkset.getSubjectsDistributionTarget());

			DistributionMongoDBObject distributionObject = new DistributionMongoDBObject(
					linkset.getObjectsDistributionTarget());

			for (String d1 : distributionSubject.getDefaultDatasets()) {
				for (String d2 : distributionObject.getDefaultDatasets()) {
					addLinksetToModel(d2,d1,linkset.getLinks());
				}
			}

		}

	}


	private void addDistributionToModel(DistributionMongoDBObject distribution) {
		// add distribution to jena model
		Resource r = outModel.createResource(distribution.getDownloadUrl());
		r.addProperty(RDFProperties.type,
				ResourceFactory.createResource(NS.DCAT_URI + "distribution"));

		String name;

		if (distribution.getTitle() == null)
			name = distribution.getUri();
		else
			name = distribution.getTitle();

		r.addProperty(RDFProperties.title, name);
	}

	private void addDatasetToModel(DatasetMongoDBObject dataset, String subset) {
		// add distribution to jena model
		Resource r = outModel.createResource(dataset.getUri());
		r.addProperty(RDFProperties.type,
				ResourceFactory.createResource(NS.VOID_URI + "Dataset"));

		String name;

		if (dataset.getTitle() == null)
			name = dataset.getUri();
		else
			name = dataset.getTitle();

		r.addProperty(RDFProperties.title, name);
		r.addProperty(RDFProperties.triples, String.valueOf(DatasetQueries.getNumberOfTriples(dataset)));
		r.addProperty(RDFProperties.subset,outModel.createResource(subset));
	}

	private void addLinksetToModel(String source, String target, int links) {

		 DatasetMongoDBObject datasetSource = new DatasetMongoDBObject(source);
		 DatasetMongoDBObject datasetTarget = new DatasetMongoDBObject(target);

		 if(!datasetSource.getIsVocabulary() && !datasetTarget.getIsVocabulary()){
		// add linksets
		String linksetURI = target+"_"+source;
		Resource r = outModel.createResource(linksetURI);
		Resource wasDerivedFrom = outModel
				.createResource(dynlod.server.ServiceAPI.getServerURL());

		r.addProperty(RDFProperties.type,
				ResourceFactory.createResource(NS.VOID_URI + "Linkset"));
		r.addProperty(ResourceFactory.createProperty(NS.VOID_URI
				+ "objectsTarget"), ResourceFactory.createResource(source));
		r.addProperty(ResourceFactory.createProperty(NS.VOID_URI
				+ "subjectsTarget"), ResourceFactory.createResource(target));
		r.addProperty(RDFProperties.wasDerivedFrom, wasDerivedFrom);
		r.addProperty(ResourceFactory.createProperty(NS.VOID_URI + "triples"),
				ResourceFactory.createPlainLiteral(String.valueOf(links)));
		
//		describe dadaset with this uri as subset
		addDatasetToModel(datasetSource, linksetURI);
		addDatasetToModel(datasetTarget, linksetURI);
		 }
		
	}

	@Test
//	public void t(){
//		outModelInit();
//		getDatasetChildren(new DatasetMongoDBObject("http://gerbil.aksw.org/gerbil/dataId/corpora/N3-RSS-500#dataset"));
//		printModel();
//	}
	
	public void getDatasetChildren(DatasetMongoDBObject d) {
		for (String child : d.getSubsetsURIs()) {
			DatasetMongoDBObject datasetChild = new DatasetMongoDBObject(child);
			getDatasetChildren(datasetChild);
			addDatasetToModel(d, child);
		}

		for (String dist : d.getDistributionsURIs()) {
			retrieveByDistribution(dist);
		}

	}

}
