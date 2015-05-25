package dynlod.download;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.rio.turtle.TurtleParser;

import dynlod.DynlodGeneralProperties;
import dynlod.threads.GetDomainsFromTriplesThread;
import dynlod.threads.SplitAndStoreThread2;
import dynlod.utils.FileUtils;
import dynlod.utils.Formats;

public class DownloadAndSaveDistribution2 extends Download {

	final static Logger logger = Logger
			.getLogger(DownloadAndSaveDistribution2.class);

	// Paths
	public String hashFileName = null;
	public String objectFilePath;

	public double contentLengthAfterDownloaded = 0;
	public Integer subjectLines = 0;
	public Integer objectLines = 0;
	public Integer totalTriples;

	// control bytes to show percentage
	public double countBytesReaded = 0;

	public ConcurrentHashMap<String, Integer> objectDomains = new ConcurrentHashMap<String, Integer>();
	public ConcurrentHashMap<String, Integer> subjectDomains = new ConcurrentHashMap<String, Integer>();
	public ConcurrentHashMap<String, Integer> countObjectDomainsHashMap = new ConcurrentHashMap<String, Integer>();
	public ConcurrentHashMap<String, Integer> countSubjectDomainsHashMap = new ConcurrentHashMap<String, Integer>();

	public AtomicInteger aint = new AtomicInteger(0);

	ConcurrentLinkedQueue<String> bufferQueue = new ConcurrentLinkedQueue<String>();
	ConcurrentLinkedQueue<String> objectQueue = new ConcurrentLinkedQueue<String>();
	ConcurrentLinkedQueue<String> subjectQueue = new ConcurrentLinkedQueue<String>();

	boolean doneReadingFile = false;
	boolean doneSplittingString = false;
	boolean doneAuthorityObject = false;
	
	public DownloadAndSaveDistribution2(String accessURL, String RDFFormat) throws MalformedURLException {
		this.url = new URL(accessURL);
		this.RDFFormat  = RDFFormat;
	}

	public void downloadDistribution() throws Exception {

		openStream();

		// allowing bzip2 format
		checkBZip2InputStream();
		
		// allowing gzip format
		checkGZipInputStream();
		
		// allowing zip format
		checkZipInputStream();

		checkTarInputStream();

		hashFileName = FileUtils.stringToHash(url.toString());
		objectFilePath = DynlodGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH
				+ hashFileName;


		setExtension(Formats.getEquivalentFormat(getExtension()));
		
		
		if(RDFFormat == null || RDFFormat.equals(""))
			RDFFormat = getExtension();
		
		DownloadDistribution();

		doneReadingFile = true;

		// update file length
		File f = new File(DynlodGeneralProperties.DUMP_PATH +hashFileName);
		if (httpContentLength < 1) {
			httpContentLength = f.length();
		}
		
		
		httpConn.disconnect();
		inputStream.close();
	}
	
	
	private void DownloadDistribution() throws IOException, InterruptedException{

//		SplitAndStoreThread splitThread = new SplitAndStoreThread(
//				bufferQueue, subjectQueue, objectQueue, getFileName());
		
		SplitAndStoreThread2 splitThread = new SplitAndStoreThread2(
				 subjectQueue, objectQueue, FileUtils.stringToHash(url.toString()));
		
		GetDomainsFromTriplesThread getDomainFromObjectsThread = new GetDomainsFromTriplesThread(
				objectQueue, countObjectDomainsHashMap);
		getDomainFromObjectsThread.start();

		GetDomainsFromTriplesThread getDomainFromSubjectsThread = new GetDomainsFromTriplesThread(
				subjectQueue, countSubjectDomainsHashMap);
		getDomainFromSubjectsThread.start();
		
		try {

			RDFParser rdfParser = null;
			
			
			if(RDFFormat.equals(Formats.DEFAULT_TURTLE)){
				rdfParser = new TurtleParser();
				logger.info("TurtleParser loaded.");
			}
			else if(RDFFormat.equals(Formats.DEFAULT_NTRIPLES)){
				rdfParser = new NTriplesParser();
				logger.info("NTriplesParser loaded.");
			}
			else if(RDFFormat.equals(Formats.DEFAULT_RDFXML)){
				rdfParser = new RDFXMLParser();
				logger.info("RDFXMLParser loaded.");
			}
			else{
				httpConn.disconnect();
				inputStream.close();
				logger.info("RDF format not supported: " + getExtension());
				throw new Exception("RDF format not supported: " + getExtension());
			}

			rdfParser.setRDFHandler(splitThread);

			try {
				rdfParser.parse(inputStream,url.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}

			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
		
		
		

		doneReadingFile = true;

		// telling thread that we are done streaming
		splitThread.setDoneReadingFile(true);

//		fileName = splitThread.getFileName();
		objectLines = splitThread.getObjectLines();
		subjectLines = splitThread.getSubjectLines();
		totalTriples = splitThread.getTotalTriples();

		getDomainFromObjectsThread.setDoneSplittingString(true);
		getDomainFromObjectsThread.join();

		getDomainFromSubjectsThread.setDoneSplittingString(true);
		getDomainFromSubjectsThread.join();
		
		splitThread.closeQueues();

		Iterator it = countObjectDomainsHashMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			if ((Integer) pair.getValue() > 50) {
				if (((String) pair.getKey()).length() < 100) {
					objectDomains.put((String) pair.getKey(),
							(Integer) pair.getValue());
				}
			}
			it.remove(); // avoids a ConcurrentModificationException
		}

		it = countSubjectDomainsHashMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			if ((Integer) pair.getValue() > 50) {
				if (((String) pair.getKey()).length() < 100) {
					subjectDomains.put((String) pair.getKey(),
							(Integer) pair.getValue());
				}
			}
			it.remove(); // avoids a ConcurrentModificationException
		}	
	}

}