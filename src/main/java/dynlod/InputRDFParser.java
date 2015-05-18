package dynlod;

import java.io.File;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import dynlod.mongodb.objects.APIStatusMongoDBObject;
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.ontology.Dataset;
import dynlod.ontology.NS;
import dynlod.ontology.RDFProperties;
import dynlod.utils.FileUtils;
import dynlod.utils.Formats;

public class InputRDFParser {

	final static Logger logger = Logger.getLogger(InputRDFParser.class);

	private Model inModel = ModelFactory.createDefaultModel();
	public List<DistributionMongoDBObject> distributionsLinks = new ArrayList<DistributionMongoDBObject>();
	int numberOfDistributions = 0;
	public boolean someDownloadURLFound = false;
	private String datasetURI;
	private String dataIDURL;
	private String access_url;

	APIStatusMongoDBObject apiStatus = null;

	boolean isVoid = false;
	boolean isDataid = false;

	public StmtIterator getFirstStmt() {
		// select dataset
		StmtIterator datasetsStmt = null;

		// find primaryTopic
		datasetsStmt = inModel.listStatements(null, RDFProperties.primaryTopic,
				(RDFNode) null);

		Resource topic = null;

		if (datasetsStmt.hasNext())
			topic = datasetsStmt.next().getObject().asResource();

		for (Resource datasetResource : RDFProperties.Dataset) {
			if (topic == null)
				datasetsStmt = inModel.listStatements(null, Dataset.type,
						datasetResource);
			else
				datasetsStmt = inModel.listStatements(topic, Dataset.type,
						datasetResource);
			if (datasetsStmt.hasNext()) {
				if (datasetResource.equals(RDFProperties.dataIdDataset))
					isDataid = true;
				break;
			}
		}

		return datasetsStmt;
	}

	public List<DistributionMongoDBObject> parseDistributions() {

		// this.distributionsLinks = distributionsLinks;
		// this.bean = bean;

		// select dataset
		StmtIterator datasetsStmt = getFirstStmt();

		if (datasetsStmt.hasNext())
			iterateSubsetsNew(datasetsStmt, null, null);

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

			// setting TOP dataset
			if (topDataset == null) {
				topDataset = datasetURI;
			}

			// create a mongodb dataset object
			DatasetMongoDBObject datasetMongoDBObj = new DatasetMongoDBObject(
					datasetURI);
			datasetMongoDBObj.addParentDatasetURI(parentDataset);
			datasetMongoDBObj.setAccess_url(access_url);

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

				// case is a Linkset subset, leave.
				StmtIterator stmtLinkset;
				stmtLinkset = inModel.listStatements(subset.getObject()
						.asResource(), RDFProperties.type,
						RDFProperties.linkset);
				if (!stmtLinkset.hasNext()) {

					datasetMongoDBObj.addSubsetURI(subset.getObject()
							.toString());
					datasetMongoDBObj.updateObject(true);

					StmtIterator stmtDatasets3 = null;

					for (Resource datasetsp : RDFProperties.Dataset) {
						stmtDatasets3 = inModel.listStatements(subset
								.getObject().asResource(), RDFProperties.type,
								datasetsp);
						if (stmtDatasets3.hasNext())
							break;
					}

					if (stmtDatasets3.hasNext())
						iterateSubsetsNew(stmtDatasets3, datasetURI, topDataset);
				}
			}

			// find a distribution within subset
			StmtIterator stmtDistribution = null;

			for (Property distributionProperty : RDFProperties.distribution) {
				stmtDistribution = inModel.listStatements(dataset.getSubject()
						.asResource(), distributionProperty, (RDFNode) null);
				if (stmtDistribution.hasNext()
						&& distributionProperty.equals(ResourceFactory
								.createProperty(NS.VOID_URI, "dataDump"))) {
					Statement stmtDistribution2 = stmtDistribution.next();
					addDistribution(stmtDistribution2, stmtDistribution2,
							datasetMongoDBObj, topDataset);
				} else if (stmtDistribution.hasNext()) {
					break;
				}
			}

			// case there's an distribution take the fist that has
			// downloadURL
			boolean downloadURLFound = false;
			while (stmtDistribution.hasNext() && downloadURLFound == false) {
				// store distribution
				Statement distributionStmt = stmtDistribution.next();

				// give priority for nt files (case it's a dataid file)
				if (isDataid) {
					if (!stmtDistribution.hasNext()
							|| distributionStmt.getObject().toString()
									.contains(".nt")) {
						// find downloadURL property
						StmtIterator stmtDownloadURL = null;

						for (Property downloadProperty : RDFProperties.downloadURL) {
							stmtDownloadURL = inModel.listStatements(
									distributionStmt.getObject().asResource(),
									downloadProperty, (RDFNode) null);
							if (stmtDownloadURL.hasNext())
								break;
						}

						// case there is an downloadURL property
						while (stmtDownloadURL.hasNext()) {
							// store downloadURL statement
							Statement downloadURLStmt = stmtDownloadURL.next();

							try {
								if (FileUtils.acceptedFormats(downloadURLStmt
										.getObject().toString())) {

									downloadURLFound = true;
									addDistribution(downloadURLStmt,
											distributionStmt,
											datasetMongoDBObj, topDataset);

								}
							} catch (Exception ex) {
								ex.printStackTrace();
								apiStatus.setHasError(true);
								apiStatus.setMessage(ex.getMessage());
							}
						}
						break;
					}
				}

				else {

					// find downloadURL property
					StmtIterator stmtDownloadURL = null;

					for (Property downloadProperty : RDFProperties.downloadURL) {
						stmtDownloadURL = inModel.listStatements(
								distributionStmt.getObject().asResource(),
								downloadProperty, (RDFNode) null);
						if (stmtDownloadURL.hasNext())
							break;
					}

					// case there is an downloadURL property
					while (stmtDownloadURL.hasNext()) {
						// store downloadURL statement
						Statement downloadURLStmt = stmtDownloadURL.next();

						try {
							if (FileUtils.acceptedFormats(downloadURLStmt
									.getObject().toString())) {

								downloadURLFound = true;
								addDistribution(downloadURLStmt,
										distributionStmt, datasetMongoDBObj,
										topDataset);

							}
						} catch (Exception ex) {
							ex.printStackTrace();
							apiStatus.setHasError(true);
							apiStatus.setMessage(ex.getMessage());
						}
					}
				}
			}
		}
	}

	public void addDistribution(Statement downloadURLStmt,
			Statement stmtDistribution, DatasetMongoDBObject subsetMongoDBObj,
			String topDataset) {

		logger.info("Distribution found: downloadURL: "
				+ downloadURLStmt.getObject().toString());

		// save distribution with downloadURL to list
		numberOfDistributions++;
		someDownloadURLFound = true;

		// creating mongodb distribution object
		DistributionMongoDBObject distributionMongoDBObj = new DistributionMongoDBObject(
				downloadURLStmt.getObject().toString());

		distributionMongoDBObj.setResourceUri(stmtDistribution.getSubject()
				.toString());

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
			distributionMongoDBObj.setFormat(Formats
					.getEquivalentFormat(stmtDistribution.getSubject()
							.getProperty(RDFProperties.format).getObject()
							.toString()));
		}
		if (stmtDistribution.getObject().asResource()
				.getProperty(RDFProperties.format) != null) {
			distributionMongoDBObj.setFormat(Formats
					.getEquivalentFormat(stmtDistribution.getObject()
							.asResource().getProperty(RDFProperties.format)
							.getObject().toString()));
		}
		if (distributionMongoDBObj.getStatus() == null) {
			distributionMongoDBObj
					.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_DOWNLOAD);
		}
		distributionMongoDBObj.updateObject(true);
		distributionsLinks.add(distributionMongoDBObj);

		if (subsetMongoDBObj != null) {
			// update dataset or subset on mongodb with distribution
			subsetMongoDBObj.addDistributionURI(downloadURLStmt.getObject()
					.toString());
			subsetMongoDBObj.updateObject(true);
		}

	}

	// read dataID file and return the dataset uri
	public String readModel(String URL, String format) throws Exception {
		apiStatus = new APIStatusMongoDBObject(URL);
		access_url = URL;
		String name = null;
		format = getJenaFormat(format);
		logger.info("Trying to read dataset: " + URL.toString());

		HttpURLConnection URLConnection = (HttpURLConnection) new URL(URL)
				.openConnection();
		URLConnection.setRequestProperty("Accept", "application/rdf+xml");

		inModel.read(URLConnection.getInputStream(), null, format);

		ResIterator hasSomeDatasets = null;
		for (Resource datasetResource : RDFProperties.Dataset) {
			hasSomeDatasets = inModel.listResourcesWithProperty(Dataset.type,
					datasetResource);
			if (hasSomeDatasets.hasNext())
				break;
		}

		if (hasSomeDatasets.hasNext()) {
			name = hasSomeDatasets.next().getURI().toString();
			logger.info("Jena model created. ");
			logger.info("Looks that this is a valid VoID/DataID file! " + name);
			apiStatus
					.setMessage("Looks that this is a valid VoID/DataID file! "
							+ name);

			dataIDURL = FileUtils.stringToHash(URL);
			inModel.write(new FileOutputStream(new File(
					DynlodGeneralProperties.DATAID_PATH + dataIDURL)));
		}
		if (name == null) {
			apiStatus.setMessage("It's not possible to find a dataset.");
			apiStatus.setHasError(true);
			throw new Exception("It's not possible to find a dataset.");
		}

		return name;
	}

	public String getJenaFormat(String format) {
		format = Formats.getEquivalentFormat(format);
		if (format.equals(Formats.DEFAULT_NTRIPLES))
			return "N-TRIPLES";

		else if (format.equals(Formats.DEFAULT_TURTLE))
			return "TTL";
		else
			return "RDF/XML";

	}

}
