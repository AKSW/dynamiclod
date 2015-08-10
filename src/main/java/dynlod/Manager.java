package dynlod;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.log4j.Logger;

import dynlod.download.CheckWhetherDownload;
import dynlod.download.StreamAndCompareDistribution;
import dynlod.exceptions.DynamicLODFileNotAcceptedException;
import dynlod.exceptions.DynamicLODNoDatasetFoundException;
import dynlod.exceptions.DynamicLODNoDistributionFoundException;
import dynlod.exceptions.DynamicLODNoDownloadURLFoundException;
import dynlod.filters.FileToFilter;
import dynlod.filters.GoogleBloomFilter;
import dynlod.lov.LOV;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.SystemPropertiesMongoDBObject;
import dynlod.utils.FileUtils;
import dynlod.utils.Timer;

public class Manager {
	final static Logger logger = Logger.getLogger(Manager.class);

	private String someDatasetURI = null;

	// list of subset and their distributions
	public List<DistributionMongoDBObject> distributionsLinks = new ArrayList<DistributionMongoDBObject>();

	InputRDFParser fileInputParserModel = new InputRDFParser();

	public void streamAndCreateFilters() throws Exception {
		// if there is at least one distribution, load them
		Iterator<DistributionMongoDBObject> distributions = distributionsLinks
				.iterator();

		int counter = 0;

		logger.info("Loading " + distributionsLinks.size()
				+ " distributions...");

		while (distributions.hasNext()) {
			counter++;

			DistributionMongoDBObject distributionMongoDBObj = distributions
					.next();

			// case there is no such distribution, create one.
			if (distributionMongoDBObj.getStatus() == null) {
				distributionMongoDBObj
						.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_STREAM);
			}

			// check is distribution need to be streamed
			boolean needDownload = checkDistributionStatus(distributionMongoDBObj);
//			needDownload = true;

			logger.info("Distribution n. " + counter + ": "
					+ distributionMongoDBObj.getUri());

			if (!needDownload) {
				logger.info("Distribution is already in the last version. No needs to stream again. ");
				distributionMongoDBObj.setLastMsg("Distribution is already in the last version. No needs to stream again.");
				distributionMongoDBObj.updateObject(true);
			}

			// if distribution have not already been handled
			if (needDownload)
				try {

					// uptate status of distribution to streaming
					distributionMongoDBObj
							.setStatus(DistributionMongoDBObject.STATUS_STREAMING);
					distributionMongoDBObj.updateObject(true);

					// now we need to download the distribution
					StreamAndCompareDistribution downloadedFile = new StreamAndCompareDistribution(
							distributionMongoDBObj.getDownloadUrl(),
							distributionMongoDBObj.getFormat(),
							distributionMongoDBObj.getUri());

					logger.info("Streaming distribution.");

					downloadedFile.downloadDistribution();

					// uptate status of distribution
					distributionMongoDBObj
							.setStatus(DistributionMongoDBObject.STATUS_STREAMED);
					distributionMongoDBObj.updateObject(true);

					logger.info("Distribution streamed. ");

					// uptate status of distribution
					distributionMongoDBObj
							.setStatus(DistributionMongoDBObject.STATUS_CREATING_BLOOM_FILTER);
					distributionMongoDBObj.updateObject(true);

					logger.info("Creating bloom filter.");

					createBloomFilter(downloadedFile, distributionMongoDBObj);

					// save distribution in a mongodb object

					logger.info("Saving mongodb \"Distribution\" document.");

					distributionMongoDBObj.setNumberOfObjectTriples(String
							.valueOf(downloadedFile.objectLines));
					distributionMongoDBObj.setDownloadUrl(downloadedFile.url
							.toString());
					distributionMongoDBObj.setFormat(downloadedFile.extension
							.toString());
					distributionMongoDBObj.setHttpByteSize(String
							.valueOf((int) downloadedFile.httpContentLength));
					distributionMongoDBObj
							.setHttpFormat(downloadedFile.httpContentType);
					distributionMongoDBObj
							.setHttpLastModified(downloadedFile.httpLastModified);
					distributionMongoDBObj
							.setObjectPath(downloadedFile.objectFilePath);
					distributionMongoDBObj
							.setTriples(downloadedFile.totalTriples);

					// remove old domains object
					// ObjectId id = new ObjectId();
					// DistributionObjectDomainsMongoDBObject d2 = new
					// DistributionObjectDomainsMongoDBObject(
					// id.get().toString());
					// d2.setDistributionURI(distributionMongoDBObj.getUri());
					// d2.remove();
					// // save object domains
					// int count = 0;
					// Iterator it = downloadedFile.objectDomains.entrySet()
					// .iterator();
					// while (it.hasNext()) {
					// Map.Entry pair = (Map.Entry) it.next();
					// String d = (String) pair.getKey();
					// // distributionMongoDBObj.addAuthorityObjects(d);
					// count++;
					// if (count % 100000 == 0) {
					// logger.debug(count
					// + " different objects domain saved ("
					// + (downloadedFile.objectDomains.size() - count)
					// + " remaining).");
					// }
					//
					// id = new ObjectId();
					// d2 = new DistributionObjectDomainsMongoDBObject(id
					// .get().toString());
					// d2.setObjectDomain(d);
					// d2.setDistributionURI(distributionMongoDBObj.getUri());
					//
					// d2.updateObject(false);
					// }
					//
					// // remove old subjects domains
					// id = new ObjectId();
					// DistributionSubjectDomainsMongoDBObject d3 = new
					// DistributionSubjectDomainsMongoDBObject(
					// id.get().toString());
					// d3.setDistributionURI(distributionMongoDBObj.getUri());
					// d3.remove();
					//
					// // save subject domains
					// count = 0;
					// it = downloadedFile.subjectDomains.entrySet().iterator();
					// while (it.hasNext()) {
					// Map.Entry pair = (Map.Entry) it.next();
					// String d = (String) pair.getKey();
					// // distributionMongoDBObj.addAuthorityObjects(d);
					// count++;
					// if (count % 100000 == 0) {
					// logger.debug(count
					// + " different subjects domain saved ("
					// + (downloadedFile.subjectDomains.size() - count)
					// + " remaining).");
					// }
					//
					// id = new ObjectId();
					// d3 = new DistributionSubjectDomainsMongoDBObject(id
					// .get().toString());
					// d3.setSubjectDomain(d);
					// d3.setDistributionURI(distributionMongoDBObj.getUri());
					//
					// d3.updateObject(false);
					// }
					//
					// logger.info(downloadedFile.objectDomains.size()
					// + " different objects domain saved.");
					//
					// logger.info(downloadedFile.subjectDomains.size()
					// + " different subjects domain saved.");

					distributionMongoDBObj.setSuccessfullyDownloaded(true);
					distributionMongoDBObj.updateObject(true);
					// bean.updateDistributionList = true;

					logger.info("Done streaming mongodb distribution object.");

					// uptate status of distribution
					distributionMongoDBObj
							.setStatus(DistributionMongoDBObject.STATUS_DONE);
					distributionMongoDBObj.updateObject(true);

					logger.info("Distribution saved! ");

				} catch (Exception e) {
					// uptate status of distribution
					distributionMongoDBObj
							.setStatus(DistributionMongoDBObject.STATUS_ERROR);
					distributionMongoDBObj.setLastMsg(e.getMessage());

					e.printStackTrace();
					distributionMongoDBObj.setSuccessfullyDownloaded(false);
					distributionMongoDBObj.updateObject(true);
				}

		}
		logger.info("We are done reading your distributions.");
	}

	public Manager(List<DistributionMongoDBObject> distributionsLinks) {
		this.distributionsLinks = distributionsLinks;
		checkLOV();
		try {
			streamAndCreateFilters();
		}
	
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public Manager(String URL) {
		try {
			logger.debug("Loading VoID/DCAT/DataID file. URL: " + URL);

			// check file extension
			FileUtils.acceptedFormats(URL.toString());

			// create jena model
			someDatasetURI = fileInputParserModel.readModel(URL, "ttl");

			logger.info("We found at least one dataset: " + someDatasetURI);

			logger.info("Parsing model in order to find distributions...");

			// parse model in order to find distributions
			List<DistributionMongoDBObject> listOfSubsets = fileInputParserModel
					.parseDistributions();
			int numberOfDistributions = listOfSubsets.size();

			if (!fileInputParserModel.someDownloadURLFound)
				throw new DynamicLODNoDownloadURLFoundException(
						"No DownloadURL property found!");
			else if (numberOfDistributions == 0)
				throw new DynamicLODNoDistributionFoundException(
						"### 0 distribution found! ###");

			checkLOV();

			// try to load distributions and make filters
			streamAndCreateFilters();

		} catch (DynamicLODFileNotAcceptedException e1) {
			logger.error(e1.getMessage());

		} catch (DynamicLODNoDatasetFoundException e2) {
			logger.error(e2.getMessage());

		} catch (DynamicLODNoDownloadURLFoundException e3) {
			logger.error(e3.getMessage());

		} catch (DynamicLODNoDistributionFoundException e4) {
			logger.error(e4.getMessage());

		} catch (Exception e5) {
			e5.printStackTrace();
			logger.error(e5.getMessage());
		}
		
		logger.info("END");
	}

	private void checkLOV() {
		// check if LOV have already been downloaded
		SystemPropertiesMongoDBObject g = new SystemPropertiesMongoDBObject();
		if (g.getDownloadedLOV() == null || !g.getDownloadedLOV()) {
			logger.info("LOV vocabularies still not lodaded! Loading now...");
			try {
				new LOV().loadLOVVocabularies();
				g.setDownloadedLOV(true);
				g.updateObject(true);
				logger.info("LOV vocabularies loaded!");
			} catch (Exception e) {
				e.printStackTrace();
				g.setDownloadedLOV(false);
				g.updateObject(true);
				logger.info("We got an error trying to load LOV vocabularies! "
						+ e.getMessage());
			}
		}
	}

	private boolean checkDistributionStatus(
			DistributionMongoDBObject distributionMongoDBObj) throws Exception {
		boolean needDownload = false;

		if (distributionMongoDBObj.getStatus().equals(
				DistributionMongoDBObject.STATUS_WAITING_TO_STREAM))
			needDownload = true;
		else if (distributionMongoDBObj.getStatus().equals(
				DistributionMongoDBObject.STATUS_STREAMING))
			needDownload = false;
		else if (distributionMongoDBObj.getStatus().equals(
				DistributionMongoDBObject.STATUS_ERROR))
			needDownload = true;
		else if (new CheckWhetherDownload()
				.checkDistribution(distributionMongoDBObj))
			needDownload = true;

		return needDownload;
	}

	public boolean createBloomFilter(
			StreamAndCompareDistribution downloadedFile,
			DistributionMongoDBObject distributionMongoDBObj) {
		GoogleBloomFilter filterSubject;
		GoogleBloomFilter filterObject;
		if (downloadedFile.subjectLines != 0) {

			// get customized equation from properties file
			if (DynlodGeneralProperties.FPP_EQUATION != null) {

				// equation parser
				JexlEngine jexl = new JexlEngine();
				jexl.setCache(512);
				jexl.setLenient(false);
				jexl.setSilent(false);

				String calc = DynlodGeneralProperties.FPP_EQUATION;
				Expression e = jexl.createExpression(calc);

				// populate the context
				JexlContext context = new MapContext();
				context.set("distributionSize", downloadedFile.subjectLines);

				// create filter for subjects
				Double result = (Double) e.evaluate(context);

				filterSubject = new GoogleBloomFilter(
						(int) downloadedFile.subjectLines, result);

				logger.info("Created bloom filter with customized equation: "
						+ DynlodGeneralProperties.FPP_EQUATION + " and value: "
						+ result);

				// create filter for objects
				filterObject = new GoogleBloomFilter(
						(int) downloadedFile.objectLines, result);

			} else {

				if (downloadedFile.subjectLines > 1000000) {
					filterSubject = new GoogleBloomFilter(
							(int) downloadedFile.subjectLines,
							0.9 / downloadedFile.subjectLines);
					filterObject = new GoogleBloomFilter(
							(int) downloadedFile.objectLines,
							0.9 / downloadedFile.objectLines);

				} else {
					filterSubject = new GoogleBloomFilter(
							(int) downloadedFile.subjectLines, 0.0000001);
					filterObject = new GoogleBloomFilter(
							(int) downloadedFile.objectLines, 0.0000001);
				}
			}
		} else {
			filterSubject = new GoogleBloomFilter(
					(int) downloadedFile.contentLengthAfterDownloaded / 40,
					0.000001);
			filterObject = new GoogleBloomFilter(
					(int) downloadedFile.contentLengthAfterDownloaded / 40,
					0.000001);
		}

		// load file to filter and take the process time
		FileToFilter f = new FileToFilter();

		Timer timer = new Timer();
		timer.startTimer();

		// Loading subject file to filter
		f.loadFileToFilter(filterSubject,
				DynlodGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH
						+ downloadedFile.hashFileName);
		distributionMongoDBObj.setTimeToCreateSubjectFilter(String
				.valueOf(timer.stopTimer()));

		filterSubject
				.saveFilter(DynlodGeneralProperties.SUBJECT_FILE_FILTER_PATH
						+ downloadedFile.hashFileName);
		// save filter

		distributionMongoDBObj
				.setSubjectFilterPath(DynlodGeneralProperties.SUBJECT_FILE_FILTER_PATH
						+ downloadedFile.hashFileName);
		distributionMongoDBObj.setNumberOfSubjectTriples(String
				.valueOf(f.elementsLoadedIntoFilter));

		timer = new Timer();
		timer.startTimer();
		// Loading object file to filter
		f.loadFileToFilter(filterObject,
				DynlodGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH
						+ downloadedFile.hashFileName);
		distributionMongoDBObj.setTimeToCreateObjectFilter(String.valueOf(timer
				.stopTimer()));

		filterObject.saveFilter(DynlodGeneralProperties.OBJECT_FILE_FILTER_PATH
				+ downloadedFile.hashFileName);
		// save filter

		distributionMongoDBObj
				.setObjectFilterPath(DynlodGeneralProperties.OBJECT_FILE_FILTER_PATH
						+ downloadedFile.hashFileName);
		distributionMongoDBObj.setNumberOfObjectTriples(String
				.valueOf(f.elementsLoadedIntoFilter));

		return false;
	}

}
