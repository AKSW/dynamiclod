package dynlod.download;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.junit.Test;

import dynlod.filters.FileToFilter;
import dynlod.filters.GoogleBloomFilter;
import dynlod.threads.SplitAndStoreThread;
import dynlod.utils.Formats;

public class DownloadLOVVocabularies extends Download {

	final static Logger logger = Logger
			.getLogger(DownloadLOVVocabularies.class);

	ConcurrentLinkedQueue<String> subjectQueue = new ConcurrentLinkedQueue<String>();
	ConcurrentLinkedQueue<String> bufferQueue = new ConcurrentLinkedQueue<String>();
	public ConcurrentHashMap<String, Integer> countSubjectDomainsHashMap = new ConcurrentHashMap<String, Integer>();
	
	public SplitAndStoreThread splitThread = null;
	
	public double contentLengthAfterDownloaded = 0;
	public double countBytesReaded = 0;
	int aux;

	public void downloadLOV(String url) throws Exception {
		this.url = new URL(url);
		openStream();

		// allowing bzip2 format
		checkGZipInputStream();
		
		if (!Formats.getEquivalentFormat(getExtension()).equals(
				Formats.DEFAULT_NQUADS)){
			throw new Exception("Format different then expected");
		}

		final byte[] buffer = new byte[BUFFER_SIZE];

		String str = "";
		int n = 0;
		BufferedInputStream b = new BufferedInputStream(inputStream);
		splitThread = new SplitAndStoreThread(
				subjectQueue, null, getFileName(), false); 


			while (-1 != (n = b.read(buffer))) {

				str = new String(buffer, 0, n);
				bufferQueue.add(str);
				str = "";

				// don't allow queue size bigger than 900;
				while (bufferQueue.size() > 900) {
					Thread.sleep(1);
				}
				
				contentLengthAfterDownloaded = contentLengthAfterDownloaded + n;

				countBytesReaded = countBytesReaded + n;

				if (aux % 1000 == 0) {
					logger.info(countBytesReaded / 1024 / 1024
							+ "MB uncompressed/lodaded.");
					aux = 0;
				}
				aux++;
				
			}
			splitThread.setDoneReadingFile(true);
	
	}

}
