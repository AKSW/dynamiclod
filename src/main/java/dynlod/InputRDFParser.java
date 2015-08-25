package dynlod;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.riot.RiotException;
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

import dynlod.exceptions.DynamicLODFormatNotAcceptedException;
import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.exceptions.DynamicLODNoDatasetFoundException;
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
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
	private String fileURLHash;
	private String access_url;

//	APIStatusMongoDBObject apiStatus = null;

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
				datasetsStmt = inModel.listStatements(null, RDFProperties.type,
						datasetResource);
			else
				datasetsStmt = inModel.listStatements(topic,
						RDFProperties.type, datasetResource);
			if (datasetsStmt.hasNext()) {
				if (datasetResource.equals(RDFProperties.dataIdDataset))
					isDataid = true;
				break;
			}
		}

		return datasetsStmt;
	}

	public List<DistributionMongoDBObject> parseDistributions()
			throws DynamicLODNoDatasetFoundException,
			DynamicLODFormatNotAcceptedException, DynamicLODGeneralException {
		// select dataset
		StmtIterator datasetsStmt = getFirstStmt();

		if (datasetsStmt.hasNext())
			iterateSubsetsNew(datasetsStmt, 0, 0, true);
		else
			throw new DynamicLODNoDatasetFoundException(
					"We could not parse any datasets.");

		return distributionsLinks;
	}

	// iterating over the subsets (recursive method)
	private void iterateSubsetsNew(StmtIterator stmtDatasets,
			int parentDataset, int topDatasetID, boolean isTopDataset)
			throws DynamicLODGeneralException,
			DynamicLODFormatNotAcceptedException {

		// iterate over subsets
		while (stmtDatasets.hasNext()) {

			// get subset
			Statement dataset = stmtDatasets.next();

			String datasetURI = dataset.getSubject().toString();
			logger.info("Found dataset: " + datasetURI);


			// create a mongodb dataset object
			DatasetMongoDBObject datasetMongoDBObj = new DatasetMongoDBObject(
					datasetURI);
			
			// do not overlap LOV datasets
			if(datasetMongoDBObj.getIsVocabulary())
				break;
			
			// setting TOP dataset
			if (isTopDataset) {
				topDatasetID = datasetMongoDBObj.getDynLodID();
			}
			

			datasetMongoDBObj.setAccess_url(access_url);

			// add description file path
			datasetMongoDBObj.setDescriptionFileName(fileURLHash);

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
			datasetMongoDBObj.addParentDatasetID(parentDataset);

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

					StmtIterator stmtDatasets3 = null;

					for (Resource datasetsp : RDFProperties.Dataset) {
						stmtDatasets3 = inModel.listStatements(subset
								.getObject().asResource(), RDFProperties.type,
								datasetsp);
						if (stmtDatasets3.hasNext())
							break;
					}

					if (stmtDatasets3.hasNext()) {
						iterateSubsetsNew(stmtDatasets3, datasetMongoDBObj.getDynLodID(), topDatasetID, false);
//						datasetMongoDBObj.addSubsetID(subset.getObject()
//								.toString());
						datasetMongoDBObj.addSubsetID(new DatasetMongoDBObject(subset.getObject()
								.toString()).getDynLodID());
						datasetMongoDBObj.updateObject(true);
					}
				}
			}

			// find a distribution within subset
			StmtIterator stmtDistribution = null;

			for (Property distributionProperty : RDFProperties.distribution) {
				stmtDistribution = inModel.listStatements(dataset.getSubject()
						.asResource(), distributionProperty, (RDFNode) null);

				// special treatment for VOID file
				if (stmtDistribution.hasNext()
						&& distributionProperty.equals(ResourceFactory
								.createProperty(NS.VOID_URI, "dataDump"))) {
					Statement stmtDistribution2 = stmtDistribution.next();
					addDistribution(stmtDistribution2, stmtDistribution2,
							datasetMongoDBObj, topDatasetID);
				} else if (stmtDistribution.hasNext()) {
					break;
				}
			}

			// case there's an distribution take the fist that has
			// downloadURL
			boolean downloadURLFound = false;
			while (stmtDistribution.hasNext()) {
				// store distribution
				Statement distributionStmt = stmtDistribution.next();

				// give priority for nt files (case it's a dataid file)
				if (isDataid) {
					if (downloadURLFound == false)
						if (!stmtDistribution.hasNext()
								|| distributionStmt.getObject().toString()
										.contains(".nt")) {
							// find downloadURL property
							StmtIterator stmtDownloadURL = null;

							for (Property downloadProperty : RDFProperties.downloadURL) {
								stmtDownloadURL = inModel.listStatements(
										distributionStmt.getObject()
												.asResource(),
										downloadProperty, (RDFNode) null);
								if (stmtDownloadURL.hasNext())
									break;
							}

							// case there is an downloadURL property
							while (stmtDownloadURL.hasNext()) {
								// store downloadURL statement
								Statement downloadURLStmt = stmtDownloadURL
										.next();

								try {
									if (FileUtils
											.acceptedFormats(downloadURLStmt
													.getObject().toString())) {

										downloadURLFound = true;
										addDistribution(downloadURLStmt,
												distributionStmt,
												datasetMongoDBObj, topDatasetID);

									}
								} catch (Exception ex) {
									ex.printStackTrace();
//									apiStatus.setHasError(true);
//									apiStatus.setMessage(ex.getMessage());
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
										topDatasetID);

							}
						} catch (DynamicLODFormatNotAcceptedException ex) {
							ex.printStackTrace();
						}
					}
				}
			}
		}
	}

	public void addDistribution(Statement downloadURLStmt,
			Statement stmtDistribution, DatasetMongoDBObject subsetMongoDBObj,
			int topDataset) {

		logger.info("Distribution found: downloadURL: "
				+ downloadURLStmt.getObject().toString());

		// save distribution with downloadURL to list
		numberOfDistributions++;
		someDownloadURLFound = true;

		// creating mongodb distribution object
		DistributionMongoDBObject distributionMongoDBObj = new DistributionMongoDBObject(
				downloadURLStmt.getObject().toString());

		// do not overlap LOV datasets
		if(distributionMongoDBObj.getIsVocabulary())
			return;
		
		distributionMongoDBObj.setResourceUri(stmtDistribution.getSubject()
				.toString());
 
		distributionMongoDBObj.addDefaultDataset(subsetMongoDBObj.getDynLodID());

		distributionMongoDBObj.setTopDataset(topDataset); 

		distributionMongoDBObj.setDownloadUrl(downloadURLStmt.getObject()
				.toString());

		// case there is title property
		try {
			if (stmtDistribution.getSubject().getProperty(RDFProperties.title) != null) {
				distributionMongoDBObj.setTitle(stmtDistribution
						.getProperty(RDFProperties.title).getObject()
						.toString());

			}
		} catch (Exception e) {
			if (stmtDistribution.getSubject().getProperty(RDFProperties.title) != null) {
				distributionMongoDBObj.setTitle(stmtDistribution.getSubject()
						.getProperty(RDFProperties.title).getObject()
						.toString());

			}
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

			// try to get format like CKAN's provides:
			// dct:format [
			// a dct:IMT ;
			// rdf:value "application/rdf+xml" ;
			// rdfs:label "application/rdf+xml"
			// ] ;
			// a dcat:Distribution ;
			// dcat:accessURL
			// <http://download.geonames.org/all-geonames-rdf.zip>

			try {
				if (stmtDistribution.getObject().asResource()
						.getProperty(RDFProperties.format).getObject()
						.asResource().getProperty(RDFProperties.rdfValue)
						.getObject() != null) {
					distributionMongoDBObj.setFormat(Formats
							.getEquivalentFormat(stmtDistribution.getObject()
									.asResource()
									.getProperty(RDFProperties.format)
									.getObject().asResource()
									.getProperty(RDFProperties.rdfValue)
									.getObject().toString()));
					
					
					
				}
			} catch (Exception e) {

				// else
				distributionMongoDBObj.setFormat(Formats
						.getEquivalentFormat(stmtDistribution.getObject()
								.asResource().getProperty(RDFProperties.format)
								.getObject().toString()));
			}

		}

		if (distributionMongoDBObj.getStatus() == null) {
			distributionMongoDBObj
					.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_STREAM);
		}
		distributionMongoDBObj.updateObject(true);
		distributionsLinks.add(distributionMongoDBObj);

		if (subsetMongoDBObj != null) {
			// update dataset or subset on mongodb with distribution
			subsetMongoDBObj.addDistributionID(distributionMongoDBObj.getDynLodID());
			subsetMongoDBObj.updateObject(true);
		}

	}

	private ResIterator findDataset() {
		ResIterator hasSomeDataset = null;
		for (Resource datasetResource : RDFProperties.Dataset) {
			hasSomeDataset = inModel.listResourcesWithProperty(
					RDFProperties.type, datasetResource);
			if (hasSomeDataset.hasNext())
				break;
		}
		return hasSomeDataset;
	}

	// read dataID file and return the dataset uri
	public String readModel(String URL, String format)
			throws MalformedURLException, IOException,
			DynamicLODNoDatasetFoundException, RiotException {
//		apiStatus = new APIStatusMongoDBObject(URL);
		access_url = URL;
		String someDatasetURI = null;
		format = getJenaFormat(format);
		logger.info("Trying to read dataset: " + URL.toString());

		HttpURLConnection URLConnection = (HttpURLConnection) new URL(URL)
				.openConnection();
		URLConnection.setRequestProperty("Accept", "application/rdf+xml");

		inModel.read(URLConnection.getInputStream(), null, format);

		ResIterator hasSomeDataset = findDataset();

		if (hasSomeDataset.hasNext()) {
			someDatasetURI = hasSomeDataset.next().getURI().toString();
			logger.info("Jena model created. ");
			logger.info("Looks that this is a valid VoID/DCAT/DataID file! "
					+ someDatasetURI);
//			apiStatus
//					.setMessage("Looks that this is a valid VoID/DCAT/DataID file! "
//							+ someDatasetURI);

			fileURLHash = FileUtils.stringToHash(URL);
			inModel.write(new FileOutputStream(new File(
					DynlodGeneralProperties.FILE_URL_PATH + fileURLHash)));
		} else {
//			apiStatus
//					.setMessage("It's not possible to find a dataset.  Perhaps that's not a valid VoID, DCAT or DataID file.");
//			apiStatus.setHasError(true);
			throw new DynamicLODNoDatasetFoundException(
					"It's not possible to find a dataset.  Perhaps that's not a valid VoID, DCAT or DataID file.");
		}

		return someDatasetURI;
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
