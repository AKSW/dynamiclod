package dynlod.API;

import java.util.ArrayList;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.queries.LinksetQueries;
import dynlod.mongodb.queries.Queries;
import dynlod.ontology.Dataset;
import dynlod.ontology.NS;
import dynlod.ontology.RDFProperties;

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

		if (LinksetQueries.checkIfDistributionExists(URI)) {
			retrieveByDistribution();

		} else if (LinksetQueries.checkIfDatasetExists(URI)) {
			retrieveByDataset();
		}

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
			ArrayList<String> DatasetList = Queries.getMongoDBObject(
					DatasetMongoDBObject.COLLECTION_NAME,
					DatasetMongoDBObject.URI, distribution.getTopDataset());
			if (DatasetList.size() > 0) {
				DatasetMongoDBObject d = new DatasetMongoDBObject(
						DatasetList.iterator().next());
				if (!d.getIsVocabulary()) {
					addDistributionToModel(distribution);
					addDistributionLinksetToModel(linkset);
				}
			}
		}
		// add linksets to jena model
		for (LinksetMongoDBObject linkset : out) {
			DistributionMongoDBObject distribution = new DistributionMongoDBObject(
					linkset.getSubjectsDistributionTarget());
			ArrayList<String> DatasetList = Queries.getMongoDBObject(
					DatasetMongoDBObject.COLLECTION_NAME,
					DatasetMongoDBObject.URI, distribution.getTopDataset());
			if (DatasetList.size() > 0) {
				DatasetMongoDBObject d = new DatasetMongoDBObject(
						DatasetList.iterator().next());
				if (!d.getIsVocabulary()) {
					addDistributionToModel(distribution);
					addDistributionLinksetToModel(linkset);
				}
			}
		}

	}

	public void retrieveByDataset() {

		// get indegree and outdegree for a distribution
		ArrayList<LinksetMongoDBObject> in = LinksetQueries
				.getLinksetsInDegreeByDataset(URI);
		ArrayList<LinksetMongoDBObject> out = LinksetQueries
				.getLinksetsOutDegreeByDataset(URI);

		// add choosen distribution to jena
		addDatasetToModel(new DatasetMongoDBObject(URI));

		// add linksets to jena model
		for (LinksetMongoDBObject linkset : in) {
			DatasetMongoDBObject dataset = new DatasetMongoDBObject(
					linkset.getObjectsDatasetTarget());
			if (!dataset.getIsVocabulary()) {
				addDatasetToModel(dataset);
				addDatasetLinksetToModel(linkset);
			}
		}
		// add linksets to jena model
		for (LinksetMongoDBObject linkset : out) {
			DatasetMongoDBObject dataset = new DatasetMongoDBObject(
					linkset.getSubjectsDatasetTarget());
			if (!dataset.getIsVocabulary()) {
				addDatasetToModel(dataset);
				addDatasetLinksetToModel(linkset);
			}
		}

	}

	private void addDistributionToModel(DistributionMongoDBObject distribution) {
		// add distribution to jena model
		Resource r = outModel.createResource(distribution.getDownloadUrl());
		r.addProperty(Dataset.type,
				ResourceFactory.createResource(NS.DCAT_URI + "distribution"));

		String name;

		if (distribution.getTitle() == null)
			name = distribution.getUri();
		else
			name = distribution.getTitle();

		r.addProperty(Dataset.title, name);
	}

	private void addDatasetToModel(DatasetMongoDBObject dataset) {
		// add distribution to jena model
		Resource r = outModel.createResource(dataset.getUri());
		r.addProperty(Dataset.type,
				ResourceFactory.createResource(NS.VOID_URI + "Dataset"));

		String name;

		if (dataset.getTitle() == null)
			name = dataset.getUri();
		else
			name = dataset.getTitle();

		r.addProperty(Dataset.title, name);
	}

	private void addDistributionLinksetToModel(LinksetMongoDBObject linkset) {
		// add linksets
		Resource r = outModel.createResource(linkset.getUri());
		Resource wasDerivedFrom = outModel.createResource(dynlod.server.ServiceAPI
				.getServerURL());

		r.addProperty(Dataset.type,
				ResourceFactory.createResource(NS.VOID_URI + "Linkset"));
		r.addProperty(ResourceFactory.createProperty(NS.VOID_URI
				+ "objectsTarget"), ResourceFactory.createResource(linkset
				.getSubjectsDistributionTarget().toString()));
		r.addProperty(ResourceFactory.createProperty(NS.VOID_URI
				+ "subjectsTarget"), ResourceFactory.createResource(linkset
				.getObjectsDistributionTarget().toString()));
		r.addProperty(RDFProperties.wasDerivedFrom, wasDerivedFrom);
		r.addProperty(ResourceFactory.createProperty(NS.VOID_URI + "triples"),
				ResourceFactory.createPlainLiteral(String.valueOf(linkset
						.getLinks())));
	}

	private void addDatasetLinksetToModel(LinksetMongoDBObject linkset) {
		// add linksets
		Resource r = outModel.createResource(linkset.getUri());
		Resource wasDerivedFrom = outModel.createResource(dynlod.server.ServiceAPI
				.getServerURL());
		r.addProperty(Dataset.type,
				ResourceFactory.createResource(NS.VOID_URI + "Linkset"));
		r.addProperty(ResourceFactory.createProperty(NS.VOID_URI
				+ "objectsTarget"), ResourceFactory.createResource(linkset
				.getSubjectsDatasetTarget().toString()));
		r.addProperty(ResourceFactory.createProperty(NS.VOID_URI
				+ "subjectsTarget"), ResourceFactory.createResource(linkset
				.getObjectsDatasetTarget().toString()));
		r.addProperty(RDFProperties.wasDerivedFrom, wasDerivedFrom);
		r.addProperty(ResourceFactory.createProperty(NS.VOID_URI + "triples"),
				ResourceFactory.createPlainLiteral(String.valueOf(linkset
						.getLinks())));
	}

}