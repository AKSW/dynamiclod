package dynlod.download;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.jsonld.JSONLDParser;
import org.openrdf.rio.n3.N3ParserFactory;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.rio.turtle.TurtleParser;

import dynlod.DynlodGeneralProperties;
import dynlod.exceptions.DynamicLODFormatNotAcceptedException;
import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.linksets.MakeLinksetsMasterThread;
import dynlod.mongodb.collections.DistributionDB;
import dynlod.mongodb.collections.RDFResources.allPredicates.AllPredicatesDB;
import dynlod.mongodb.collections.RDFResources.allPredicates.AllPredicatesRelationDB;
import dynlod.mongodb.collections.RDFResources.owlClass.OwlClassDB;
import dynlod.mongodb.collections.RDFResources.owlClass.OwlClassRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfDB;
import dynlod.mongodb.collections.RDFResources.rdfSubClassOf.RDFSubClassOfRelationDB;
import dynlod.mongodb.collections.RDFResources.rdfType.RDFTypeObjectDB;
import dynlod.mongodb.collections.RDFResources.rdfType.RDFTypeObjectRelationDB;
import dynlod.mongodb.queries.PredicatesQueries;
import dynlod.parsers.NTriplesDynLODParser;
import dynlod.threads.SplitAndStoreThread;
import dynlod.utils.FileUtils;
import dynlod.utils.Formats;

public class StreamAndCompareDistribution extends Stream {

	final static Logger logger = Logger
			.getLogger(StreamAndCompareDistribution.class);

	// Paths
	public String hashFileName = null;
	public String objectFilePath;
	public String uri;

	public double contentLengthAfterDownloaded = 0;
	public Integer subjectLines = 0;
	public Integer objectLines = 0;
	public Integer totalTriples;

	ConcurrentLinkedQueue<String> bufferQueue = new ConcurrentLinkedQueue<String>();
	ConcurrentLinkedQueue<String> objectQueue = new ConcurrentLinkedQueue<String>();
	ConcurrentLinkedQueue<String> subjectQueue = new ConcurrentLinkedQueue<String>();

	boolean doneReadingFile = false;
	boolean doneSplittingString = false;
	boolean doneAuthorityObject = false;

	public MakeLinksetsMasterThread getDomainFromObjectsThread = null;
	public MakeLinksetsMasterThread getDomainFromSubjectsThread = null;
	
	private DistributionDB distribution = null;

	public StreamAndCompareDistribution(DistributionDB distributionMongoDBObj) throws MalformedURLException {
		this.distribution = distributionMongoDBObj;
		this.url = new URL(distributionMongoDBObj.getDownloadUrl());
		this.RDFFormat = distributionMongoDBObj.getFormat();
		this.uri = distributionMongoDBObj.getUri();
	}

	public void streamDistribution() throws IOException,
			DynamicLODGeneralException, InterruptedException,
			RDFHandlerException, RDFParseException,
			DynamicLODFormatNotAcceptedException {

		openStream();

		// allowing bzip2 format
		checkBZip2InputStream();

		// allowing gzip format
		checkGZipInputStream();

		hashFileName = FileUtils.stringToHash(url.toString());
		objectFilePath = DynlodGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH
				+ hashFileName;

		if (RDFFormat == null || RDFFormat.equals("")) {
			DistributionDB dist = new DistributionDB(
					url.toString());
			if (dist.getFormat() == null || dist.getFormat() == ""
					|| dist.getFormat().equals(""))
				RDFFormat = getExtension();
			else
				RDFFormat = dist.getFormat();
		}
		StreamDistribution();

		// setExtension(Formats.getEquivalentFormat(getExtension()));

		doneReadingFile = true;

		// update file length
		File f = new File(DynlodGeneralProperties.DUMP_PATH + hashFileName);
		if (httpContentLength < 1) {
			httpContentLength = f.length();
		}

		httpConn.disconnect();
		inputStream.close();
	}

	private void StreamDistribution() throws InterruptedException,
			DynamicLODGeneralException, 
			DynamicLODFormatNotAcceptedException {

//		SplitAndStoreThread splitThread = new SplitAndStoreThread(subjectQueue,
//				objectQueue, FileUtils.stringToHash(url.toString()));
		SplitAndStoreThread splitThread = new SplitAndStoreThread(subjectQueue,
				objectQueue, distribution.getTitle()+"_"+distribution.getDynLodID());

		getDomainFromObjectsThread = new MakeLinksetsMasterThread(objectQueue,
				uri);
		getDomainFromSubjectsThread = new MakeLinksetsMasterThread(subjectQueue,
				uri);
		
		getDomainFromObjectsThread.setName("getDomainFromObjectsThread");		
		getDomainFromSubjectsThread.isSubject = true;
		getDomainFromSubjectsThread.setName("getDomainFromSubjectsThread");

		try {

			RDFParser rdfParser = null;

			if (RDFFormat.equals(Formats.DEFAULT_TURTLE)) {
				rdfParser = new TurtleParser();
				logger.info("==== Turtle Parser loaded ====");
			} else if (RDFFormat.equals(Formats.DEFAULT_NTRIPLES)) {
				// rdfParser = new NTriplesParser();
				rdfParser = new NTriplesDynLODParser();
				logger.info("==== NTriples Parser loaded ====");
			} else if (RDFFormat.equals(Formats.DEFAULT_RDFXML)) {
				rdfParser = new RDFXMLParser();
				logger.info("==== RDF/XML Parser loaded ====");
			} else if (RDFFormat.equals(Formats.DEFAULT_JSONLD)) {
				rdfParser = new JSONLDParser();
				logger.info("==== JSON-LD Parser loaded ====");
			} else if (RDFFormat.equals(Formats.DEFAULT_N3)) {
				rdfParser = new N3ParserFactory().getParser();
				logger.info("==== N3Parser loaded ====");
			} else {
				httpConn.disconnect();
				inputStream.close();
				logger.info("RDF format not supported: " + RDFFormat);
				throw new DynamicLODFormatNotAcceptedException(
						"RDF format not supported: " + RDFFormat);
			}
//			getDomainFromSubjectsThread.start();
//			getDomainFromObjectsThread.start();

			rdfParser.setRDFHandler(splitThread);
			ParserConfig config = new ParserConfig();
			config.set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);
			config.set(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES, false);
			config.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
			config.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
			config.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);
			rdfParser.setParserConfig(config);

			// check whether file is tar/zip type
			if (getExtension().equals("zip")) {
				InputStream data = new BufferedInputStream(inputStream);
				logger.info("File extension is zip, creating ZipInputStream and checking compressed files...");

				ZipInputStream zip = new ZipInputStream(data);
				int nf = 0;
				ZipEntry entry = zip.getNextEntry();
				while (entry != null) {
					if (!entry.isDirectory()) {
						logger.info(++nf + " zip file uncompressed.");
						logger.info("File name: " + entry.getName());

						// byte[] content = new byte[(int) entry.getSize()];

						// zip.read(content, 0, (int) entry.getSize());

						rdfParser.parse(zip, url.toString());

						// BufferedReader in=new BufferedReader(new
						// InputStreamReader(entry));

					}

					entry = zip.getNextEntry();
				}

				setExtension(FilenameUtils.getExtension(getFileName()));
			}

			else if (getExtension().equals("tar")) {
				InputStream data = new BufferedInputStream(inputStream);
				logger.info("File extension is tar, creating TarArchiveInputStream and checking compressed files...");

				TarArchiveInputStream tar = new TarArchiveInputStream(data);
				int nf = 0;
				TarArchiveEntry entry = (TarArchiveEntry) tar.getNextEntry();
				while (entry != null) {
					if (entry.isFile() && !entry.isDirectory()) {
						logger.info(++nf + " tar file uncompressed.");
						logger.info("File name: " + entry.getName());

						byte[] content = new byte[(int) entry.getSize()];

						tar.read(content, 0, (int) entry.getSize());

						rdfParser.parse(tar, url.toString());

					}

					entry = (TarArchiveEntry) tar.getNextEntry();
				}

				setExtension(FilenameUtils.getExtension(getFileName()));
			}

			else {
				rdfParser.parse(inputStream, url.toString());

			}

		} catch (RDFHandlerException | IOException | RDFParseException e) {

		}

		doneReadingFile = true;

		// fileName = splitThread.getFileName();
		objectLines = splitThread.getObjectLines();
		subjectLines = splitThread.getSubjectLines();
		totalTriples = splitThread.getTotalTriples();

		getDomainFromObjectsThread.setDoneSplittingString(true);
		getDomainFromObjectsThread.join();

		getDomainFromSubjectsThread.setDoneSplittingString(true);
		getDomainFromSubjectsThread.join();

		splitThread.closeQueues();

		
		logger.info("Saving predicates...");
		// save predicates
//		new PredicatesQueries().insertPredicates(splitThread.predicates, distribution.getDynLodID(), distribution.getTopDataset());
		new AllPredicatesDB().insertSet(splitThread.allPredicates.keySet());
		new AllPredicatesRelationDB().insertSet(splitThread.allPredicates, distribution.getDynLodID(), distribution.getTopDataset());
		
		logger.info("Saving RDF TYPE objects...");
		// Saving RDF Type classes
		new RDFTypeObjectDB().insertSet(splitThread.rdfTypeObjects.keySet());
		new RDFTypeObjectRelationDB().insertSet(splitThread.rdfTypeObjects,  distribution.getDynLodID(), distribution.getTopDataset());

		new RDFSubClassOfDB().insertSet(splitThread.rdfSubClassOf.keySet());
		new RDFSubClassOfRelationDB().insertSet(splitThread.rdfSubClassOf, distribution.getDynLodID(), distribution.getTopDataset());
		
		new OwlClassDB().insertSet(splitThread.owlClasses.keySet());
		new OwlClassRelationDB().insertSet(splitThread.owlClasses, distribution.getDynLodID(), distribution.getTopDataset());
			
	}

}