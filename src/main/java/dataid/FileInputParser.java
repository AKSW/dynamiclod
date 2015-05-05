package dataid;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.sun.istack.logging.Logger;

import dataid.exceptions.DataIDException;
import dataid.mongodb.objects.DatasetMongoDBObject;
import dataid.mongodb.objects.DistributionMongoDBObject;
import dataid.ontology.Dataset;
import dataid.ontology.Distribution;
import dataid.ontology.NS;
import dataid.ontology.RDFProperties;
import dataid.server.DataIDBean;
import dataid.utils.FileUtils;
import dataid.utils.Formats;

public class FileInputParser {

	final static Logger logger = Logger.getLogger(FileInputParser.class);

	private Model inModel = ModelFactory.createDefaultModel();
	List<DistributionMongoDBObject> distributionsLinks;
	int numberOfDistributions = 0;
	public boolean someDownloadURLFound = false;
	private String datasetURI;
	private String dataIDURL;
	DataIDBean bean;

	boolean isVoid = false;

	public List<DistributionMongoDBObject> parseDistributions(
			List<DistributionMongoDBObject> distributionsLinks, DataIDBean bean) {

		this.distributionsLinks = distributionsLinks;
		this.bean = bean;

		// select dataset
		StmtIterator datasetsStmt = null;

		for (Resource datasetResource : RDFProperties.Dataset) {
			datasetsStmt = inModel.listStatements(null, Dataset.type,
					datasetResource);
			if (datasetsStmt.hasNext()) {
				break;
			}
		}
		if (datasetsStmt.hasNext())
			iterateSubsetsNew(datasetsStmt, null,null);


		return distributionsLinks;
	}

	// iterating over the subsets (recursive method)
	private void iterateSubsetsNew(StmtIterator stmtDatasets,
			String parentDataset, String topDataset) {

		// iterate over subsets
		while (stmtDatasets.hasNext()) {

			// get subset
			Statement dataset = stmtDatasets.next();

			String datasetURI = dataset.getSubject().toString();
			logger.info("Found datasetset: " + datasetURI);
			
			//setting TOP dataset
			if(topDataset==null){
				topDataset = datasetURI;
			}

			// create a mongodb dataset object
			DatasetMongoDBObject datasetMongoDBObj = new DatasetMongoDBObject(
					datasetURI);
			datasetMongoDBObj.addParentDatasetURI(parentDataset);

			// add DataID file path
			datasetMongoDBObj.setDataIdFileName(dataIDURL);

			// case there is title property
			if (dataset.getSubject().getProperty(RDFProperties.title) != null) {
				datasetMongoDBObj.setTitle(dataset.getSubject()
						.getProperty(RDFProperties.title).getObject()
						.toString());
			} else
				datasetMongoDBObj.setTitle(datasetURI);

			// case there is label property
			if (dataset.getSubject().getProperty(RDFProperties.label) != null) {
				datasetMongoDBObj.setLabel(dataset.getSubject()
						.getProperty(RDFProperties.label).getObject()
						.toString());
			} else
				datasetMongoDBObj.setLabel(datasetURI);

			datasetMongoDBObj.updateObject(true);

			// find subset within subset
			StmtIterator stmtDatasets2 = inModel.listStatements(dataset
					.getSubject().asResource(), RDFProperties.subset,
					(RDFNode) null);

			// case there is a subset, call method recursively
			while (stmtDatasets2.hasNext()) {

				// get subset
				Statement subset = stmtDatasets2.next();

				datasetMongoDBObj.addSubsetURI(subset.getObject().toString());
				datasetMongoDBObj.updateObject(true);

				StmtIterator stmtDatasets3 = null;

				for (Resource datasetsp : RDFProperties.Dataset) {
					stmtDatasets3 = inModel.listStatements(subset.getObject()
							.asResource(), RDFProperties.type, datasetsp);
					if (stmtDatasets3.hasNext())
						break;
				}

				if (stmtDatasets3.hasNext())
					iterateSubsetsNew(stmtDatasets3, datasetURI, topDataset);
			}

			// find a distribution within subset
			StmtIterator stmtDistribution = null;

			for (Property distributionProperty : RDFProperties.distribution) {
				stmtDistribution = inModel.listStatements(dataset.getSubject()
						.asResource(), distributionProperty, (RDFNode) null);
				if (stmtDistribution.hasNext() && distributionProperty.equals(ResourceFactory.createProperty(NS.VOID_URI, "dataDump")))
				{
					Statement stmtDistribution2 = stmtDistribution.next();
					addDistribution(stmtDistribution2, stmtDistribution2, datasetMongoDBObj, topDataset);
				}
				else if(stmtDistribution.hasNext())
					break;
			}

			// case there's an distribution take the fist that has
			// downloadURL
			boolean downloadURLFound = false;
			if (stmtDistribution.hasNext() && downloadURLFound == false) {
				// store distribution
				Statement distributionStmt = stmtDistribution.next();

				// find downloadURL property
				StmtIterator stmtDownloadURL = null;

				for (Property downloadProperty : RDFProperties.downloadURL) {
					stmtDownloadURL = inModel.listStatements(distributionStmt
							.getObject().asResource(), downloadProperty,
							(RDFNode) null);
					if (stmtDownloadURL.hasNext())
						break;
				}
				

				// case there is an downloadURL property
				if (stmtDownloadURL.hasNext()) {
					downloadURLFound = true;
					// store downloadURL statement
					Statement downloadURLStmt = stmtDownloadURL.next();
					try {
						if (FileUtils.acceptedFormats(downloadURLStmt.getObject()
								.toString())) {

							addDistribution(downloadURLStmt, distributionStmt,
									datasetMongoDBObj, topDataset);
						}
					} catch (DataIDException ex) {
						ex.printStackTrace();
					}
				}
			}
		}

	}

	public void addDistribution(Statement downloadURLStmt, Statement stmtDistribution,
			DatasetMongoDBObject subsetMongoDBObj,
			String topDataset) {

		bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_LOG,
				"Distribution found: downloadURL: "
						+ downloadURLStmt.getObject().toString());

		// save distribution with downloadURL to list
		numberOfDistributions++;
		someDownloadURLFound = true;

		// creating mongodb distribution object
		DistributionMongoDBObject distributionMongoDBObj = new DistributionMongoDBObject(
				stmtDistribution.getObject().toString());
		
		distributionMongoDBObj.addDefaultDataset(subsetMongoDBObj.getUri());
		
		distributionMongoDBObj.setTopDataset(topDataset);

		distributionMongoDBObj.setDownloadUrl(downloadURLStmt.getObject()
				.toString());

		// case there is title property
		if (stmtDistribution.getSubject().getProperty(RDFProperties.title) != null) {
			distributionMongoDBObj.setTitle(stmtDistribution.getSubject()
					.getProperty(RDFProperties.title).getObject().toString());
		}

		// case there is format property
		if (stmtDistribution.getSubject().getProperty(RDFProperties.format) != null) {
			distributionMongoDBObj.setFormat(Formats.getEquivalentFormat(stmtDistribution.getSubject()
					.getProperty(RDFProperties.format).getObject().toString()));
		}
		if (stmtDistribution.getObject().asResource().getProperty(RDFProperties.format) != null) {
			distributionMongoDBObj.setFormat(Formats.getEquivalentFormat(stmtDistribution.getObject()
					.asResource().getProperty(RDFProperties.format).getObject().toString()));
		}
		if (distributionMongoDBObj.getStatus() == null) {
			distributionMongoDBObj
					.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_DOWNLOAD);
		}
		distributionMongoDBObj.updateObject(true);
		distributionsLinks.add(distributionMongoDBObj);

		if (subsetMongoDBObj != null) {
			// update dataset or subset on mongodb with distribution
			subsetMongoDBObj.addDistributionURI(stmtDistribution.getSubject()
					.toString());
			subsetMongoDBObj.updateObject(true);
		} 

	}

	// read dataID file and return the dataset uri
	public String readModel(String URL, DataIDBean bean) throws Exception {
		String name = null;

		this.bean = bean;

		inModel.read(URL, null, "TTL");

		ResIterator hasSomeDatasets = null;
		for (Resource datasetResource : RDFProperties.Dataset) {
			hasSomeDatasets = inModel.listResourcesWithProperty(Dataset.type,
					datasetResource);
			if (hasSomeDatasets.hasNext())
				break;
		}

		if (hasSomeDatasets.hasNext()) {
			name = hasSomeDatasets.next().getURI().toString();
			bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_LOG,
					"Jena model created. ");
			bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_LOG,
					"Looks that this is a valid VoID/DataID file! " + name);
			dataIDURL = FileUtils.stringToHash(URL);
			inModel.write(new FileOutputStream(new File(
					DataIDGeneralProperties.DATAID_PATH + dataIDURL)));
		}
		if (name == null) {
			throw new Exception(
					"It's not possible to find a dataid:Dataset. Check your dataid namespace "
							+ NS.DATAID_URI);
		}

		return name;
	}

}
