package dataid;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import dataid.exceptions.DataIDException;
import dataid.models.DistributionModel;
import dataid.mongodb.objects.DatasetMongoDBObject;
import dataid.mongodb.objects.DistributionMongoDBObject;
import dataid.mongodb.objects.SubsetMongoDBObject;
import dataid.ontology.Dataset;
import dataid.ontology.Distribution;
import dataid.ontology.NS;
import dataid.server.DataIDBean;
import dataid.utils.FileUtils;

public class DataIDModel {

	private Model inModel = ModelFactory.createDefaultModel();
	List<DistributionModel> distributionsLinks;
	int numberOfDistributions = 0;
	public boolean someDownloadURLFound = false;
	private String datasetURI;
	private String dataIDURL;
	DataIDBean bean;

	DatasetMongoDBObject datasetMongoDBObj;

	public List<DistributionModel> parseDistributions(
			List<DistributionModel> distributionsLinks, DataIDBean bean) {
		this.distributionsLinks = distributionsLinks;
		this.bean = bean;

		// select dataset
		StmtIterator datasets = inModel.listStatements(null,
				Dataset.dataIDType, Dataset.dataIDDataset);

		while (datasets.hasNext()) {

			Statement dataset = datasets.next();
			System.out.println("We found a new dataset: " + dataset.getSubject());
			bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_LOG,"We found a new dataset: " + dataset.getSubject()+"<br>");
			
			datasetURI = dataset.getSubject().toString();

			// create a mongodb dataset object
			datasetMongoDBObj = new DatasetMongoDBObject(datasetURI);

			// add dataid path to dataset object
			datasetMongoDBObj.setDataIdFileName(dataIDURL);

			// case there is title property
			if (dataset.getSubject().getProperty(Dataset.title) != null) {
				datasetMongoDBObj.setTitle(dataset.getSubject()
						.getProperty(Dataset.title).getObject().toString());
			}
			else datasetMongoDBObj.setTitle(datasetURI);

			// case there is label property
			if (dataset.getSubject().getProperty(Dataset.label) != null) {
				datasetMongoDBObj.setLabel(dataset.getSubject()
						.getProperty(Dataset.label).getObject().toString());
			}
			else datasetMongoDBObj.setLabel(datasetURI);

			// try to find distribution within dataset
			StmtIterator stmtDistribution = inModel.listStatements(
					dataset.getSubject(), Distribution.dcatDistribution,
					(RDFNode) null);

			// case there's an distribution take the fist that has downloadURL
			boolean downloadURLFound = false;
			while (stmtDistribution.hasNext()) {
//				while (stmtDistribution.hasNext() && downloadURLFound == false) {

				// get distribution
				Statement distribution = stmtDistribution.next();

				// find downloadURL property
				StmtIterator stmtDownloadURL = inModel.listStatements(
						distribution.getObject().asResource(),
						Distribution.downloadURL, (RDFNode) null);

				bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_LOG,
						"Distribution found: "
								+ distribution.getObject().toString());

				// case there is an downloadURL property
				while (stmtDownloadURL.hasNext()) {
//					while (stmtDownloadURL.hasNext() && !downloadURLFound) {
					// store downloadURL statement
					Statement downloadURL = stmtDownloadURL.next();
					try {
						if (FileUtils.acceptedFormats(downloadURL.getObject()
								.toString())) {

							downloadURLFound = true;

							// save distribution with downloadURL to list
							distributionsLinks.add(new DistributionModel(
									numberOfDistributions, datasetURI,
									distribution.getSubject().toString(),
									distribution.getObject().toString(), downloadURL
									.getObject().toString()));
							numberOfDistributions++;
							someDownloadURLFound = true;

							bean.addDisplayMessage(
									DataIDGeneralProperties.MESSAGE_LOG,
									"Distribution DownloadURL found: "
											+ downloadURL.getObject().toString());

							// create a mongodb distribution object
							DistributionMongoDBObject distributionMongoDBObj = new DistributionMongoDBObject(
									distribution.getObject().toString());
							distributionMongoDBObj
									.addDefaultDataset(datasetMongoDBObj
											.getUri());
							distributionMongoDBObj.setDownloadUrl(downloadURL
									.getObject().toString());

							// case there is title property
							if (distribution.getSubject().getProperty(
									Distribution.title) != null) {
								distributionMongoDBObj.setTitle(distribution
										.getSubject()
										.getProperty(Distribution.title)
										.getObject().toString());
							}
							
							// case there is format property
							if (distribution.getSubject().getProperty(
									Distribution.format) != null) {
								distributionMongoDBObj.setFormat(distribution
										.getSubject()
										.getProperty(Distribution.format)
										.getObject().toString());
							}

							if(distributionMongoDBObj.getStatus() == null){
								distributionMongoDBObj.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_DOWNLOAD);
								distributionMongoDBObj.updateObject(true);
							}

							// update dataset on mongodb with distribution
							datasetMongoDBObj.addDistributionURI(distribution
									.getObject().toString());
							datasetMongoDBObj.updateObject(true);
						}
					} catch (DataIDException ex) {
						bean.addDisplayMessage(
								DataIDGeneralProperties.MESSAGE_ERROR,
								ex.getMessage());
						ex.printStackTrace();
					}

					datasetMongoDBObj.updateObject(true);

				}
			}

			// try find a subset
			StmtIterator stmtSubsets = inModel.listStatements(
					dataset.getSubject(), Dataset.subset, (RDFNode) null);
			StmtIterator stmtSubsetsTmp = inModel.listStatements(
					dataset.getSubject(), Dataset.subset, (RDFNode) null);

			// try to find a subset
			// case there is a subset, call recursive

			if (stmtSubsets.hasNext()) {

				while (stmtSubsetsTmp.hasNext()) {

					Statement s = stmtSubsetsTmp.next();
					// get subset and update mongodb parent dataset
					datasetMongoDBObj.addSubsetURI(s.getObject().toString());
					datasetMongoDBObj.updateObject(true);
				}

				iterateSubsetsNew(stmtSubsets);
			}

		}

		return distributionsLinks;
	}

	// iterating over the subsets (recursive method)
	private void iterateSubsetsNew(StmtIterator stmtSubsets) {

		// iterate over subsets
		while (stmtSubsets.hasNext()) {

			// get subset
			Statement subset = stmtSubsets.next();

			// create a mongodb subset object
			SubsetMongoDBObject subsetMongoDBObj = new SubsetMongoDBObject(
					subset.getObject().toString());
			subsetMongoDBObj
					.addParentDatasetURI(subset.getSubject().toString());
			subsetMongoDBObj.updateObject(true);

			bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_LOG,
					"Subset found: " + subset.getObject().toString());

			// find subset within subset
			StmtIterator stmtSubsets2 = inModel.listStatements(subset
					.getObject().asResource(), Dataset.subset, (RDFNode) null);

			// case there is a subset, call method recursively
			if (stmtSubsets2.hasNext()) {
				subsetMongoDBObj.addSubsetURI(subset.getSubject().toString());
				subsetMongoDBObj.updateObject(true);

				iterateSubsetsNew(stmtSubsets2);
			} else {

				// find distribution within subset
				StmtIterator stmtDistribution = inModel.listStatements(subset
						.getObject().asResource(),
						Distribution.dcatDistribution, (RDFNode) null);

				// case there's an distribution take the fist that has downloadURL
				boolean downloadURLFound = false;
				while (stmtDistribution.hasNext() && downloadURLFound == false) {
					// store distribution
					Statement distribution = stmtDistribution.next();

					bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_LOG,
							"Distribution found: "
									+ distribution.getObject().toString());

					// find downloadURL property
					StmtIterator stmtDownloadURL = inModel.listStatements(
							distribution.getObject().asResource(),
							Distribution.downloadURL, (RDFNode) null);

					// case there is an downloadURL property
					if (stmtDownloadURL.hasNext()) {
						downloadURLFound = true;
						// store downloadURL statement
						Statement downloadURL = stmtDownloadURL.next();
						bean.addDisplayMessage(
								DataIDGeneralProperties.MESSAGE_LOG,
								"Distribution found: downloadURL: "
										+ downloadURL.getObject().toString());
						try {
							if (FileUtils.acceptedFormats(downloadURL.getObject()
									.toString())) {

								// save distribution with downloadURL to list
								distributionsLinks.add(new DistributionModel(
										numberOfDistributions, datasetURI,
										downloadURL.getSubject().toString(),
										distribution.getObject().toString(),downloadURL
										.getObject().toString()));
								numberOfDistributions++;
								someDownloadURLFound = true;

								// creating mongodb distribution object
								DistributionMongoDBObject distributionMongoDBObj = new DistributionMongoDBObject(
										distribution.getObject().toString());
								distributionMongoDBObj
										.addDefaultDataset(subsetMongoDBObj
												.getUri());
								distributionMongoDBObj.setDownloadUrl(downloadURL
										.getObject().toString());
								
								
								// case there is title property
								if (distribution.getSubject().getProperty(
										Distribution.title) != null) {
									distributionMongoDBObj.setTitle(distribution
											.getSubject()
											.getProperty(Distribution.title)
											.getObject().toString());
								}
								
								// case there is format property
								if (distribution.getSubject().getProperty(
										Distribution.format) != null) {
									distributionMongoDBObj.setFormat(distribution
											.getSubject()
											.getProperty(Distribution.format)
											.getObject().toString());
								}
								if(distributionMongoDBObj.getStatus() == null){
									distributionMongoDBObj.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_DOWNLOAD);
									distributionMongoDBObj.updateObject(true);
								}

								// update dataset on mongodb with distribution
								subsetMongoDBObj
										.addDistributionURI(distribution
												.getSubject().toString());
								subsetMongoDBObj.updateObject(true);

							}
						} catch (DataIDException ex) {
							bean.addDisplayMessage(
									DataIDGeneralProperties.MESSAGE_ERROR,
									ex.getMessage());
							ex.printStackTrace();
						}
					}
				}
			}
		}

	}

	// read dataID file and return the dataset uri
	public String readModel(String URL) throws Exception {
		String name = null;

		inModel.read(URL, null, "TTL");
		ResIterator i = inModel.listResourcesWithProperty(Dataset.dataIDType,
				Dataset.dataIDDataset);
		while (i.hasNext()) {
			name = i.next().getURI().toString();
			bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_LOG,
					"Jena model created. ");
			bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_LOG,
					"Looks that this is a valid DataID file! " + name);
			dataIDURL = FileUtils.stringToHash(URL);
			inModel.write(new FileOutputStream(new File(
					DataIDGeneralProperties.DATAID_PATH + dataIDURL)));
		}
		if(name==null){
			throw new DataIDException("It's not possible to find a dataid:Dataset. Check your dataid namespace "+NS.DATAID_URI);
		}

		return name;
	}

}
