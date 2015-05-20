package dynlod.download;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import dynlod.DynlodGeneralProperties;
import dynlod.threads.GetDomainsFromTriplesThread;
import dynlod.threads.SplitAndStoreThread;
import dynlod.utils.FileUtils;
import dynlod.utils.Formats;

public class DownloadAndSaveDistribution extends Download {

	final static Logger logger = Logger
			.getLogger(DownloadAndSaveDistribution.class);

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
	
	public DownloadAndSaveDistribution(String accessURL, String RDFFormat) throws MalformedURLException {
		this.url = new URL(accessURL);
		this.RDFFormat  = RDFFormat;
	}

	public void downloadDistribution() throws Exception {

		inputStream = getStream();

		// allowing bzip2 format
		inputStream = getBZip2InputStream(inputStream);
		
		// allowing gzip format
		inputStream = getGZipInputStream(inputStream);
		
		// allowing zip format
		inputStream = getZipInputStream(inputStream);

		inputStream = getTarInputStream(inputStream);

		hashFileName = FileUtils.stringToHash(url.toString());
		objectFilePath = DynlodGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH
				+ hashFileName;


		setExtension(Formats.getEquivalentFormat(getExtension()));
		
		
		if(RDFFormat == null || RDFFormat.equals(""))
			RDFFormat = getExtension();
		
		if (Formats.getEquivalentFormat(RDFFormat).equals(
				Formats.DEFAULT_NQUADS)) {
			// parse graphs and call DownloadDistributionNTFormat()
			
		}

		else if (Formats.getEquivalentFormat(RDFFormat).equals(
				Formats.DEFAULT_NTRIPLES)) {
			DownloadDistributionNTFormat();

		} else if (Formats.getEquivalentFormat(RDFFormat).equals(
				Formats.DEFAULT_TURTLE)
				|| Formats.getEquivalentFormat(RDFFormat).equals(
						Formats.DEFAULT_RDFXML)) {
			int bytesRead = -1;
			FileOutputStream outputStream = new FileOutputStream(DynlodGeneralProperties.DUMP_PATH +hashFileName);
			while (-1 != (bytesRead = inputStream.read(buffer))) {
				outputStream.write(buffer, 0, bytesRead);
				contentLengthAfterDownloaded = contentLengthAfterDownloaded
						+ bytesRead;
				countBytesReaded = countBytesReaded + bytesRead;
				if (aux % 1000 == 0) {
					logger.info(countBytesReaded / 1024 / 1024
							+ "MB uncompressed/lodaded.");
					aux = 0;
				}
				aux++;
			}
			outputStream.close();
		} else {
			httpConn.disconnect();
			inputStream.close();
			throw new Exception("RDF format not supported: " + getExtension());
		}

		doneReadingFile = true;

		// update file length
		File f = new File(DynlodGeneralProperties.DUMP_PATH +hashFileName);
		if (httpContentLength < 1) {
			httpContentLength = f.length();
		}
		
		
		httpConn.disconnect();
		inputStream.close();
	}
	
	
	private void DownloadDistributionNTFormat() throws IOException, InterruptedException{

//		SplitAndStoreThread splitThread = new SplitAndStoreThread(
//				bufferQueue, subjectQueue, objectQueue, getFileName());
		
		SplitAndStoreThread splitThread = new SplitAndStoreThread(
				bufferQueue, subjectQueue, objectQueue, FileUtils.stringToHash(url.toString()));
		
		splitThread.start();

		GetDomainsFromTriplesThread getDomainFromObjectsThread = new GetDomainsFromTriplesThread(
				objectQueue, countObjectDomainsHashMap);
		getDomainFromObjectsThread.start();

		GetDomainsFromTriplesThread getDomainFromSubjectsThread = new GetDomainsFromTriplesThread(
				subjectQueue, countSubjectDomainsHashMap);
		getDomainFromSubjectsThread.start();

		String str = "";
		BufferedInputStream b = new BufferedInputStream(inputStream);
		while (-1 != (n = b.read(buffer))) {

			str = new String(buffer, 0, n);
			bufferQueue.add(str);
			str = "";
			contentLengthAfterDownloaded = contentLengthAfterDownloaded + n;

			countBytesReaded = countBytesReaded + n;

			if (aux % 1000 == 0) {
				logger.info(countBytesReaded / 1024 / 1024
						+ "MB uncompressed/lodaded.");
				aux = 0;
			}
			aux++;

			// don't allow queue size bigger than 900;
			while (bufferQueue.size() > 900) {
				Thread.sleep(1);
			}

		}

		doneReadingFile = true;

		// telling thread that we are done streaming
		splitThread.setDoneReadingFile(true);
		splitThread.join();

//		fileName = splitThread.getFileName();
		objectLines = splitThread.getObjectLines();
		subjectLines = splitThread.getSubjectLines();
		totalTriples = splitThread.getTotalTriples();

		getDomainFromObjectsThread.setDoneSplittingString(true);
		getDomainFromObjectsThread.join();

		getDomainFromSubjectsThread.setDoneSplittingString(true);
		getDomainFromSubjectsThread.join();

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