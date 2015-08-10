package dynlod.download;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.DecimalFormat;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

public class Download {

	final static Logger logger = Logger.getLogger(Download.class);

	// HTTP header fields
	public String httpDisposition = null;
	public String httpContentType = null;
	public double httpContentLength;
	public String httpLastModified = "0";

	protected static final int BUFFER_SIZE = 16384;
	public URL url = null;

	protected InputStream inputStream = null;

	final byte[] buffer = new byte[BUFFER_SIZE];
	int n = 0;
	int aux = 0;

	public String fileName = null;
	public String extension = null;
	public String RDFFormat = null;

	HttpURLConnection httpConn = null;

	String accessURL = null;

	protected void getMetadataFromHTTPHeaders(HttpURLConnection httpConn) {

		httpDisposition = httpConn.getHeaderField("Content-Disposition");
		httpContentType = httpConn.getContentType();
		httpContentLength = httpConn.getContentLength();
		if (httpConn.getLastModified() > 0)
			httpLastModified = String.valueOf(httpConn.getLastModified());

		printHeaders();

	}

	protected void openStream() throws Exception {
		openConnection();

		// opens input stream from HTTP connection
		InputStream inputStream = httpConn.getInputStream();

		logger.debug("InputStream from http connection opened");

		// get some data from headers
		getMetadataFromHTTPHeaders(httpConn);

		this.inputStream = inputStream;

	}

	private void openConnection() throws Exception {
		httpConn = (HttpURLConnection) url.openConnection();

		httpConn.setReadTimeout(5000);
		httpConn.setConnectTimeout(5000);
		int responseCode = httpConn.getResponseCode();

		logger.debug("Open HTTP connection for URL: " + url.toString());

		// check HTTP response code
		if (responseCode != HttpURLConnection.HTTP_OK) {
			httpConn.disconnect();
			throw new Exception(
					"No file to download. Server replied HTTP code: "
							+ responseCode);
		}
		logger.debug("Successfuly connected with HTTP OK status.");

	}

	protected void printHeaders() {
		DecimalFormat df = new DecimalFormat("#.##");

		logger.debug("Content-Type = " + httpContentType);
		logger.debug("Last-Modified = " + httpLastModified);
		logger.debug("Content-Disposition = " + httpDisposition);
		logger.debug("Content-Length = "
				+ df.format(httpContentLength / 1024 / 1024) + " MB");
		logger.debug("fileName = " + fileName);
	}

	protected void checkBZip2InputStream() throws Exception {

		// check whether file is bz2 type
		if (getExtension().equals("bz2")) {
			logger.info("File extension is bz2, creating BZip2CompressorInputStream...");
			httpConn = (HttpURLConnection) url.openConnection();
			inputStream = new BZip2CompressorInputStream(
					httpConn.getInputStream(), true);
			setFileName(getFileName().replace(".bz2", ""));
			setExtension(null);

			logger.info("Done creating BZip2CompressorInputStream! New file name is "
					+ getFileName());
		}
	}

	protected void checkGZipInputStream() throws Exception {

		// check whether file is gz type
		if (getExtension().equals("gz") || getExtension().equals("tgz")) {
			logger.info("File extension is " + getExtension()
					+ ", creating GzipCompressorInputStream...");
			logger.debug(new FileNameFromURL().getFileName(
					url.toString(), httpDisposition));
			httpConn = (HttpURLConnection) url.openConnection();
			inputStream = new GzipCompressorInputStream(
					httpConn.getInputStream(), true);
			setFileName(getFileName().replace(".gz", ""));
			setFileName(getFileName().replace(".tgz", ".tar"));
			if (getFileName().contains(".tar"))
				setExtension("tar");
			if (getExtension().equals("tgz"))
				setExtension("tar");
			else
				setExtension(null);

			logger.info("Done creating GzipCompressorInputStream! New file name is "
					+ getFileName() + ", extension: "+getExtension());
		}
	}

//	protected void checkZipInputStream() throws Exception {
//		// check whether file is zip type
//		if (getExtension().equals("zip")) {
//			logger.info("File extension is zip, creating ZipInputStream and checking compressed files...");
//			DownloadZipUtils d = new DownloadZipUtils();
//			// d.checkZipFile(url);
//			httpConn = (HttpURLConnection) url.openConnection();
//			ZipInputStream zip = new ZipInputStream(httpConn.getInputStream());
//			ZipEntry entry = zip.getNextEntry();
//			while (entry != null) {
//				if (!entry.isDirectory())
//					break;
//				else
//					entry = zip.getNextEntry();
//			}
//
//			setFileName(entry.getName());
//			setExtension(FilenameUtils.getExtension(getFileName()));
//			inputStream = zip;
//			logger.info("Done, we found a single file: " + fileName);
//
//		}
//	}

//	protected void checkTarInputStream() throws Exception {
//
//		// check whether file is zip type
//		if (getExtension().equals("tar")) {
//			InputStream data = new BufferedInputStream(inputStream);
//			logger.info("File extension is tar, creating TarArchiveInputStream and checking compressed files...");
//		
//			TarArchiveInputStream tar = new TarArchiveInputStream(data);
//			TarArchiveEntry entry = (TarArchiveEntry) tar.getNextEntry();
//			while (entry != null) {
//				if (entry.isFile() && !entry.isDirectory()) {
//					InputStream n = null;
//					n=tar;
//				
//					setFileName(entry.getName());
//				}
//
//				entry = (TarArchiveEntry) tar.getNextEntry();
//
//			}
//
//			setExtension(FilenameUtils.getExtension(getFileName()));
//			inputStream = tar;
//			logger.info("Done, we found a file: " + fileName);
//
//		}
//	}

	public String getFileName() {
		if (fileName == null) {
			// extracts file name from header field
			fileName = new FileNameFromURL().getFileName(url.toString(),
					httpDisposition);
			logger.debug("Found file name: " + fileName);
		}
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getExtension() {
		if (extension == null) {
			logger.info("Setting file extension.");
			extension = FilenameUtils.getExtension(getFileName());
			logger.info(extension);
		}
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}

	public URL getUrl() {
		return url;
	}

	public void setUrl(URL url) {
		this.url = url;
	}

	public void simpleDownload(String file, InputStream stream) {
		try {
			ReadableByteChannel rbc = Channels.newChannel(stream);
			FileOutputStream fos = new FileOutputStream(file);
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
