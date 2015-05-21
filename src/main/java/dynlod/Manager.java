package dynlod;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import dynlod.download.CheckWhetherDownload;
import dynlod.download.DownloadAndSaveDistribution;
import dynlod.files.PrepareFiles;
import dynlod.filters.FileToFilter;
import dynlod.filters.GoogleBloomFilter;
import dynlod.lov.LOV;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.DistributionObjectDomainsMongoDBObject;
import dynlod.mongodb.objects.DistributionSubjectDomainsMongoDBObject;
import dynlod.mongodb.objects.SystemPropertiesMongoDBObject;
import dynlod.utils.FileUtils;
import dynlod.utils.Formats;
import dynlod.utils.Timer;

public class Manager {
	final static Logger logger = Logger.getLogger(Manager.class);

	private String name = null;

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

			DistributionMongoDBObject distributionMongoDBObj = distributions.next();

			// case there is no such distribution, create one.
			if (distributionMongoDBObj.getStatus() == null) {
				distributionMongoDBObj
						.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_DOWNLOAD);
			}

			// check is distribution need to be streamed
			boolean needDownload = false;

			if (distributionMongoDBObj.getStatus().equals(
					DistributionMongoDBObject.STATUS_WAITING_TO_DOWNLOAD))
				needDownload = true;
			else if (distributionMongoDBObj.getStatus().equals(
					DistributionMongoDBObject.STATUS_DOWNLOADING))
				needDownload = false;
			else if (distributionMongoDBObj.getStatus().equals(
					DistributionMongoDBObject.STATUS_ERROR))
				needDownload = true;
			else if (new CheckWhetherDownload()
					.checkDistribution(distributionMongoDBObj))
				needDownload = true;

			logger.info("Distribution n. " + counter + ": "
					+distributionMongoDBObj.getUri());

			if (!needDownload) {
				logger.info("Distribution is already in the last version. No needs to download again. ");
			}

			// if distribution have not already been handled
			if (needDownload)
				try {

					// uptate status of distribution to downloading
					distributionMongoDBObj
							.setStatus(DistributionMongoDBObject.STATUS_DOWNLOADING);
					distributionMongoDBObj.updateObject(true);

					// now we need to download the distribution
					DownloadAndSaveDistribution downloadedFile = new DownloadAndSaveDistribution(
							distributionMongoDBObj.getDownloadUrl(), distributionMongoDBObj.getFormat());

					logger.info("Downloading distribution.");

					downloadedFile.downloadDistribution();

					// uptate status of distribution
					distributionMongoDBObj
							.setStatus(DistributionMongoDBObject.STATUS_DOWNLOADED);
					distributionMongoDBObj.updateObject(true);

					logger.info("Distribution downloaded. ");

					// check if format is not ntriples
					if (!downloadedFile.RDFFormat
							.equals(Formats.DEFAULT_NTRIPLES)) {

						// uptate status of distribution
						distributionMongoDBObj
						.setStatus(DistributionMongoDBObject.STATUS_SEPARATING_SUBJECTS_AND_OBJECTS);
					
						distributionMongoDBObj.updateObject(true);

						logger.info("Separating subjects and objects.");

						PrepareFiles p = new PrepareFiles();
						// separating subjects and objects using rapper and awk
						// error to convert dbpedia files from turtle using
						// rapper
						
						
						p.separateSubjectAndObject(downloadedFile.hashFileName,
								downloadedFile.RDFFormat);
						downloadedFile.objectDomains = p.objectDomains;
						downloadedFile.subjectDomains = p.subjectDomains;
						downloadedFile.objectFilePath = p.objectFile;
						downloadedFile.totalTriples = p.totalTriples;
						downloadedFile.objectLines = p.objectTriples;
						
						//remove dump file
						File f = new File(DynlodGeneralProperties.DUMP_PATH+ downloadedFile.hashFileName); 
						if(f.isFile()) f.delete();
					}

					// uptate status of distribution
					distributionMongoDBObj
							.setStatus(DistributionMongoDBObject.STATUS_CREATING_BLOOM_FILTER);
					distributionMongoDBObj.updateObject(true);

					logger.info("Creating bloom filter.");

					// make a filter with subjects
					GoogleBloomFilter filter;
					if (downloadedFile.subjectLines != 0) {
						
						// get customized equation from properties file
						if(DynlodGeneralProperties.FPP_EQUATION!=null){
							
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

								// work it out
								Double result = (Double) e.evaluate(context);								
								
							
							filter = new GoogleBloomFilter(
									(int) downloadedFile.subjectLines,
									result);
							
							logger.info("Created bloom filter with customized equation: "+DynlodGeneralProperties.FPP_EQUATION + " and value: "+ result);
						}
						else{
						
						if (downloadedFile.subjectLines > 1000000)
							filter = new GoogleBloomFilter(
									(int) downloadedFile.subjectLines,
									0.9 / downloadedFile.subjectLines);
						else
							filter = new GoogleBloomFilter(
									(int) downloadedFile.subjectLines,
									0.0000001);
						}
					} else {
						filter = new GoogleBloomFilter(
								(int) downloadedFile.contentLengthAfterDownloaded / 40,
								0.000001);
					}

					// load file to filter and take the process time
					FileToFilter f = new FileToFilter();

					Timer timer = new Timer();
					timer.startTimer();

					// Loading file to filter
					f.loadFileToFilter(filter, downloadedFile.hashFileName);
					distributionMongoDBObj.setTimeToCreateFilter(String
							.valueOf(timer.stopTimer()));

					filter.saveFilter(downloadedFile.hashFileName);
					// save filter

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
							.setSubjectFilterPath(filter.fullFilePath);
					distributionMongoDBObj
							.setNumberOfTriplesLoadedIntoFilter(String
									.valueOf(f.subjectsLoadedIntoFilter));
					distributionMongoDBObj
							.setTriples(downloadedFile.totalTriples);

					// remove old domains object
					ObjectId id = new ObjectId();
					DistributionObjectDomainsMongoDBObject d2 = new DistributionObjectDomainsMongoDBObject(
							id.get().toString());
					d2.setDistributionURI(distributionMongoDBObj.getUri());
					d2.remove();

					// save object domains
					int count = 0;
					Iterator it = downloadedFile.objectDomains.entrySet()
							.iterator();
					while (it.hasNext()) {
						Map.Entry pair = (Map.Entry) it.next();
						String d = (String) pair.getKey();
						// distributionMongoDBObj.addAuthorityObjects(d);
						count++;
						if (count % 100000 == 0) {
							logger.debug(count
									+ " different objects domain saved ("
									+ (downloadedFile.objectDomains.size() - count)
									+ " remaining).");
						}

						id = new ObjectId();
						d2 = new DistributionObjectDomainsMongoDBObject(id
								.get().toString());
						d2.setObjectDomain(d);
						d2.setDistributionURI(distributionMongoDBObj.getUri());

						d2.updateObject(false);
					}

					// remove old subjects domains
					id = new ObjectId();
					DistributionSubjectDomainsMongoDBObject d3 = new DistributionSubjectDomainsMongoDBObject(
							id.get().toString());
					d3.setDistributionURI(distributionMongoDBObj.getUri());
					d3.remove();

					// save subject domains
					count = 0;
					it = downloadedFile.subjectDomains.entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry pair = (Map.Entry) it.next();
						String d = (String) pair.getKey();
						// distributionMongoDBObj.addAuthorityObjects(d);
						count++;
						if (count % 100000 == 0) {
							logger.debug(count
									+ " different subjects domain saved ("
									+ (downloadedFile.subjectDomains.size() - count)
									+ " remaining).");
						}

						id = new ObjectId();
						d3 = new DistributionSubjectDomainsMongoDBObject(id
								.get().toString());
						d3.setSubjectDomain(d);
						d3.setDistributionURI(distributionMongoDBObj.getUri());

						d3.updateObject(false);
					}

					logger.info(downloadedFile.objectDomains.size()
							+ " different objects domain saved.");

					logger.info(downloadedFile.subjectDomains.size()
							+ " different subjects domain saved.");

					distributionMongoDBObj.setSuccessfullyDownloaded(true);
					distributionMongoDBObj.updateObject(true);
//					bean.updateDistributionList = true;

					logger.info("Done saving mongodb distribution object.");

					// uptate status of distribution
					distributionMongoDBObj
							.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_CREATE_LINKSETS);
					distributionMongoDBObj.updateObject(true);

					logger.info("Distribution saved! ");

				} catch (Exception e) {
					// uptate status of distribution
					distributionMongoDBObj
							.setStatus(DistributionMongoDBObject.STATUS_ERROR);
					distributionMongoDBObj.setLastErrorMsg(e.getMessage());
			
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Manager(String URL) {
		try {
			FileUtils.checkIfFolderExists();

			logger.debug("Loading DataID file URL: " + URL + " url.");

			// check file extension
			FileUtils.acceptedFormats(URL.toString());

			// create jena model
			name = fileInputParserModel.readModel(URL, "ttl");

			if (name == null) {
				logger.error("Impossible to read dataset. Perhaps that's not a valid DataID file. Dataset: "
						+ name);
				return;
			}

			logger.info("We found at least one dataset: " + name);

			logger.info("Parsing model in order to find distributions...");

			// parse model in order to find distributions
			List<DistributionMongoDBObject> listOfSubsets = fileInputParserModel
					.parseDistributions();
			int numberOfDistributions = listOfSubsets.size();

			// update view
			if (numberOfDistributions > 0) {
//				bean.setDownloadDatasetURI(listOfSubsets.get(0).getUri());
//				DataIDBean.pushDownloadInfo();
			}

			if (!fileInputParserModel.someDownloadURLFound)
				throw new Exception("No DownloadURL property found!");
			else if (numberOfDistributions == 0)
				throw new Exception("### 0 distribution found! ###");				

			checkLOV();
			
			// try to load distributions and make filters
			streamAndCreateFilters();

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		logger.info("END");
	}

	
	private void checkLOV(){
		// check if LOV was already downloaded
		SystemPropertiesMongoDBObject g = new SystemPropertiesMongoDBObject();
		System.out.println(g.getDownloadedLOV());
		if (g.getDownloadedLOV()== null || !g.getDownloadedLOV()){
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
					logger.info("We got an error trying to load LOV vocabularies! "+ e.getMessage());
				}
		}
	}

}
