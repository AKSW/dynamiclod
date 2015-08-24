package dynlod.API.services;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import dynlod.API.core.API;
import dynlod.exceptions.DynamicLODNoDatasetFoundException;
import dynlod.exceptions.api.DynamicLODAPINoLinksFoundException;
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.queries.DatasetQueries;
import dynlod.mongodb.queries.DistributionQueries;
import dynlod.mongodb.queries.LinksetQueries;
import dynlod.ontology.NS;
import dynlod.ontology.RDFProperties;

public class APIRetrieveRDF extends API {

	public Model outModel = null;
	
	final static Logger logger = Logger.getLogger(APIRetrieveRDF.class);

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	public APIRetrieveRDF() {
		// TODO Auto-generated constructor stub
	}

//	@Test
//	public void t() throws DynamicLODNoDatasetFoundException, DynamicLODAPINoLinksFoundException {
//		outModelInit();
//		getDatasetChildren(new DatasetMongoDBObject(
//				"http://gerbil.aksw.org/gerbil/dataId/corpora/N3-RSS-500#dataset"));
//		printModel();
//	}

	public APIRetrieveRDF(String URI) throws DynamicLODNoDatasetFoundException,
			DynamicLODAPINoLinksFoundException {
		
		outModelInit();
		
		// try to find by distribution
		DistributionMongoDBObject dist = new DistributionMongoDBObject(URI);
		
		if(dist.getDefaultDatasets().size()>0){
			retrieveByDistribution(dist.getUri());
			logger.debug("APIRetrieve found a distribution to retrieve RDF: "+ dist.getUri());
		}
		else{
			DatasetMongoDBObject d = new DatasetMongoDBObject(URI, true);
			getDatasetChildren(d);
		}
//		printModel();

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

	public void retrieveByDistribution(String distributionURI) throws DynamicLODAPINoLinksFoundException {
		// get indegree and outdegree for a distribution
		DistributionMongoDBObject dis = new DistributionMongoDBObject(distributionURI);
		DistributionQueries queries = new DistributionQueries();
		
		ArrayList<LinksetMongoDBObject> in = new LinksetQueries()
				.getLinksetsInDegreeByDistribution(dis.getDynLodID());
		ArrayList<LinksetMongoDBObject> out = new LinksetQueries()
				.getLinksetsOutDegreeByDistribution(dis.getDynLodID());

		// add choosen distribution to jena
		// addDistributionToModel(new
		// DistributionMongoDBObject(distributionURI));
		
		boolean linksetsFound = false;

		// add linksets to jena model
		for (LinksetMongoDBObject linkset : in) {
			DistributionMongoDBObject distributionSubject = new DistributionMongoDBObject(
					linkset.getDistributionTarget());

			DistributionMongoDBObject distributionObject =  new DistributionMongoDBObject(
					linkset.getDistributionSource());

			for (int d1 : distributionSubject.getDefaultDatasets()) {
				for (int d2 : distributionObject.getDefaultDatasets()) {
					if(addLinksetToModel(d2, d1, linkset.getLinks()))
						linksetsFound = true;
				}
			}
		}
		// add linksets to jena model
		for (LinksetMongoDBObject linkset : out) {
			DistributionMongoDBObject distributionSubject =  new DistributionMongoDBObject(
					linkset.getDistributionTarget());

			DistributionMongoDBObject distributionObject = new DistributionMongoDBObject(
					linkset.getDistributionSource());

			for (int d1 : distributionSubject.getDefaultDatasets()) {
				for (int d2 : distributionObject.getDefaultDatasets()) {
					if(addLinksetToModel(d2, d1, linkset.getLinks()))
						linksetsFound = true;
				}
			}

		}
		
//		if(!linksetsFound)
//			throw new DynamicLODAPINoLinksFoundException ("Your dataset still doesn't not contains links with our stored datasets.");

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
		r.addProperty(RDFProperties.triples,
				String.valueOf(new DatasetQueries().getNumberOfTriples(dataset)));
		r.addProperty(RDFProperties.subset, outModel.createResource(subset));
	}

	private boolean addLinksetToModel(int source, int target, int links) {
		DatasetMongoDBObject datasetSource = new DatasetMongoDBObject(source);
		DatasetMongoDBObject datasetTarget =new DatasetMongoDBObject(target);

		if (!datasetSource.getIsVocabulary()
				&& !datasetTarget.getIsVocabulary()) {
//			 add linksets
//			String linksetURI = target + "_" + source;
			String linksetURI = dynlod.server.ServiceAPI.getServerURL() + "?source="+
					datasetSource.getUri() + "&target="+ datasetTarget.getUri();
			Resource r = outModel.createResource(linksetURI);
			Resource wasDerivedFrom = outModel
					.createResource(dynlod.server.ServiceAPI.getServerURL());

			r.addProperty(RDFProperties.type,
					ResourceFactory.createResource(NS.VOID_URI + "Linkset"));
			r.addProperty(
					ResourceFactory.createProperty(NS.VOID_URI
							+ "objectsTarget"),
					ResourceFactory.createResource(datasetSource.getUri()));
			r.addProperty(
					ResourceFactory.createProperty(NS.VOID_URI
							+ "subjectsTarget"),
					ResourceFactory.createResource(datasetTarget.getUri()));
			r.addProperty(RDFProperties.wasDerivedFrom, wasDerivedFrom);
			r.addProperty(
					ResourceFactory.createProperty(NS.VOID_URI + "triples"),
					ResourceFactory.createPlainLiteral(String.valueOf(links)));

			// describe dadaset with this uri as subset
			addDatasetToModel(datasetSource, linksetURI);
			addDatasetToModel(datasetTarget, linksetURI);
			
			return true;
		}
		else return false;

	}

	public void getDatasetChildren(DatasetMongoDBObject d)
			throws DynamicLODNoDatasetFoundException, DynamicLODAPINoLinksFoundException {
		boolean datasetOrDistribuionFound = false;

		for (int child : d.getSubsetsIDs()) {
			DatasetMongoDBObject datasetChild = new DatasetMongoDBObject(child);
			getDatasetChildren(datasetChild);
			addDatasetToModel(d, new DatasetMongoDBObject(child).getUri());
			datasetOrDistribuionFound = true;
		}

		for (int dist : d.getDistributionsIDs()) {
			retrieveByDistribution(new  DistributionMongoDBObject(dist).getUri());
			datasetOrDistribuionFound = true;
		}

		if (!datasetOrDistribuionFound)
			throw new DynamicLODNoDatasetFoundException(
					"Not possible to find datasets, subsets or distributions within "
							+ d.getUri()+". Please enter a valid dataset URI.");

	}

}
