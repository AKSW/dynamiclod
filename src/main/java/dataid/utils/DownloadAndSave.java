package dataid.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.FilenameUtils;

import dataid.DataIDGeneralProperties;
import dataid.mongodb.objects.DistributionMongoDBObject;
import dataid.server.DataIDBean;

public class DownloadAndSave {
	private static final int BUFFER_SIZE = 2048;

	public String fileName = "";

	// HTTP header fields
	public String httpDisposition = null;
	public String httpContentType = null;
	public double httpContentLength;
	public String httpLastModified = "0";

	// Paths
	public String dataIDFilePath = null;
	public String objectFilePath;

	public URL url = null;

	public double contentLengthAfterDownloaded;
	public int subjectLines = 0;
	public int objectLines = 0;

	public ArrayList<String> authorityDomains = new ArrayList<String>();

	public AtomicInteger aint = new AtomicInteger(0);

	Queue<String> bufferQueue = new ConcurrentLinkedQueue<String>();
	Queue<String> objectQueue = new ConcurrentLinkedQueue<String>();
	boolean doneReadingFile = false;
	boolean doneSplittingString = false;

	public String downloadFile(String fileURL, DataIDBean bean)
			throws Exception {
		url = new URL(fileURL);
		HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
		int responseCode = httpConn.getResponseCode();

		// check HTTP response code first
		if (responseCode == HttpURLConnection.HTTP_OK) {

			// get some data from headers
			httpDisposition = httpConn.getHeaderField("Content-Disposition");
			httpContentType = httpConn.getContentType();
			httpContentLength = httpConn.getContentLength();
			if (httpConn.getLastModified() > 0)
				httpLastModified = String.valueOf(httpConn.getLastModified());

			// check if distribution already exists
			if (!checkWheterDownload(fileURL,
					String.valueOf(httpContentLength), httpLastModified)) {
				bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_INFO,
						"File previously downloaded. No modification found. "
								+ fileURL);
				return "";
			}

			if (httpDisposition != null) {
				// extracts file name from header field
				int index = httpDisposition.indexOf("filename=");
				if (index > 0) {
					fileName = httpDisposition.substring(index + 10,
							httpDisposition.length() - 1);
				}
			} else {
				// extracts file name from URL
				fileName = fileURL.substring(fileURL.lastIndexOf("/") + 1,
						fileURL.length());
			}
			DecimalFormat df = new DecimalFormat("#.##");

			System.out.println("Content-Type = " + httpContentType);
			System.out.println("Last-Modified = " + httpLastModified);
			System.out.println("Content-Disposition = " + httpDisposition);
			System.out.println("Content-Length = "
					+ df.format(httpContentLength / 1024 / 1024) + " MB");
			System.out.println("fileName = " + fileName);

			// opens input stream from the HTTP connection
			InputStream inputStream = httpConn.getInputStream();

			String extension = FilenameUtils.getExtension(fileName);

			// check if file is bz2 type
			if (extension.equals("bz2")) {
				inputStream = new BZip2CompressorInputStream(
						httpConn.getInputStream(), true);
				fileName = fileName.replace(".bz2", "");
			}

			dataIDFilePath = DataIDGeneralProperties.BASE_PATH + fileName;
			objectFilePath = DataIDGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH
					+ fileName;
			final byte[] buffer = new byte[BUFFER_SIZE];
			int n = 0;

			extension = FilenameUtils.getExtension(fileName);
			if (extension.equals("nt")) {
				// split_and_store.start();
				Runnable r = new SplitAndStore(bean);
				new Thread(r).start();

				 Runnable r2 = new AddAuthorityObject();
				 new Thread(r2).start();

				String str = "";
				while (-1 != (n = inputStream.read(buffer))) {

					for (int i = 0; i < n; i++) {

						str = str + (char) buffer[i];
					}
					bufferQueue.add(str);
					str = "";
					contentLengthAfterDownloaded = contentLengthAfterDownloaded
							+ n;
					aint.incrementAndGet();

					// don't allow queue size bigger than 900;
					while (bufferQueue.size() > 900) {
					}

				}
				while (bufferQueue.size() > 0)
					;

			} else if (extension.equals("ttl") || extension.equals("rdf")) {
				int bytesRead = -1;
				FileOutputStream outputStream = new FileOutputStream(
						dataIDFilePath);
				while (-1 != (bytesRead = inputStream.read(buffer))) {
					outputStream.write(buffer, 0, bytesRead);
					contentLengthAfterDownloaded = contentLengthAfterDownloaded
							+ bytesRead;
					for (int i = 0; i < bytesRead; i++) {
						if (buffer[i] == '\n')
							objectLines++;
					}
				}
			} else
				throw new Exception("RDF format not supported: " + extension);

			doneReadingFile = true;

			// update file length
			if (httpContentLength < 1) {
				File f = new File(dataIDFilePath);
				httpContentLength = f.length();
			}

			bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_LOG,
					"File downloaded: " + fileName);

			return dataIDFilePath;
		} else {
			bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_WARN,
					"No file to download. Server replied HTTP code: "
							+ responseCode);
		}
		httpConn.disconnect();
		return null;
	}

	// Thread reads string buffer, split objects and subjects, and saves in the
	// disk
	public class SplitAndStore implements Runnable {
		DataIDBean bean;

		public SplitAndStore(DataIDBean bean) {
			this.bean = bean;
		}

		public synchronized void run() {

			try {

				// creates subject file in disk
				FileOutputStream subject = new FileOutputStream(
						DataIDGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH
								+ fileName);

				// creates object file in disk
				FileOutputStream object = new FileOutputStream(
						DataIDGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH
								+ fileName);

				String lastLine = "";
				String tmpLastSubject = "";
				int count = 0;
				int ciro = 0;

				// starts reading buffer queue
				while (!doneReadingFile) {
					while (bufferQueue.size() > 0) {
						aint.decrementAndGet();
						try {
							String o[];
							o = bufferQueue.remove().split("\n");
							if (!lastLine.equals("")) {
								o[0] = lastLine.concat(o[0]);

								lastLine = "";
							}
							// System.out.println(o[0]);

							for (int q = 0; q < o.length; q++) {
								String u = o[q];
								if (!u.startsWith("#")) {
									try {

										Pattern pattern = Pattern
										// .compile("^(<[^>]+>)\\s+(<[^>]+>)\\s(.+[>|\"]).*");
										// .compile("^(<[^>]+>)\\s+(<[^>]+>)\\s(.+[>|\"].*)\\s\\.");
												.compile("^(<[^>]+>)\\s+(<[^>]+>)\\s(.*)(\\s\\.)");

										// System.out.println(u);
										Matcher matcher = pattern.matcher(u);
										if (!matcher.matches()) {
											throw new ArrayIndexOutOfBoundsException();
										}

										// get subject and save to file
										if (!tmpLastSubject.equals(matcher
												.group(1))) {
											tmpLastSubject = matcher.group(1);
											subject.write(new String(matcher
													.group(1) + "\n")
													.getBytes());
											subjectLines++;
										}

										// get object (make sure that its a
										// resource
										// and not a literal), add to queue and
										// save
										// to file
										if (!matcher.group(3).startsWith("\"")) {
											object.write(new String(matcher
													.group(3) + "\n")
													.getBytes());

											// add object to object queue (the
											// queue
											// is read by other thread)
											objectQueue.add(matcher.group(3));
											objectLines++;
										}
										count++;

										// send message to view
										if (count % 100000 == 0) {
											System.out.println(count
													+ " registers written");
											System.out
													.println("Buffer queue size: "
															+ aint.get());
											bean.setDownloadNumberOfTriplesLoaded(count);
											bean.pushDownloadInfo();
										}

									} catch (ArrayIndexOutOfBoundsException e) {
										// e.printStackTrace();
										lastLine = u;
									}
								}
							}

						} catch (NoSuchElementException em) {
							// em.printStackTrace();
						} catch (Exception e) {
							e.printStackTrace();
						}

					}
				}
				bean.setDownloadNumberOfTriplesLoaded(count);
				bean.setDownloadNumberOfDownloadedDistributions(bean
						.getDownloadNumberOfDownloadedDistributions() + 1);

				bean.pushDownloadInfo();
				object.close();
				subject.close();
				doneSplittingString = true;

			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

		}
	};

	public class AddAuthorityObject implements Runnable {

		public synchronized void run() {
			String obj = "";
			while (!doneSplittingString) {
				while (objectQueue.size() > 0) {
					obj = objectQueue.remove();
					String authority = "";

					URL url;
					try {
						obj = obj.substring(1, obj.length() - 1);
						url = new URL(obj);
						authority = url.getProtocol() + "://" + url.getHost();
					} catch (MalformedURLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (!authority.equals(""))
						if (!authorityDomains.contains(authority)) {
							authorityDomains.add(authority);
						}
				}
			}
		}
	};

	// Thread split_and_store = new Thread() {
	//
	// public synchronized void run() {
	//
	// try {
	// FileOutputStream subject = new FileOutputStream(
	// DataIDGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH
	// + fileName);
	// FileOutputStream object = new FileOutputStream(
	// DataIDGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH
	// + fileName);
	//
	// String lastLine = null;
	// String tmpLastSubject = "";
	// int count = 0;
	// String u2[];
	// String u3[];
	// String tmp;
	// String o[];
	// while (!doneReadingFile || (bufferQueue.size() > 0)) {
	// if (bufferQueue.size() > 0)
	// aint.decrementAndGet();
	// try {
	// o = bufferQueue.remove().split("\n");
	// if (lastLine != null) {
	// o[0] = lastLine.concat(o[0]);
	// lastLine = null;
	// }
	//
	// for (String u : o) {
	// if (!u.startsWith("#")) {
	// try {
	// u2 = u.split(" ");
	// u3 = u.split("> <");
	//
	// // checking here offset
	// tmp = u3[1];
	// tmp = u2[3];
	//
	// if (!tmpLastSubject.equals(u2[0])) {
	// tmpLastSubject = u2[0];
	// subject.write(new String(u2[0] + "\n")
	// .getBytes());
	// subjectLines++;
	// }
	// if (!u2[2].startsWith("\"")) {
	// object.write(new String(u2[2] + "\n")
	// .getBytes());
	// objectLines++;
	// count++;
	// }
	// if (count % 100000 == 0) {
	// System.out.println(count
	// + " registers written");
	// System.out
	// .println("Buffer queue size: "
	// + aint.get());
	// DataID.bean
	// .setDownloadNumberOfTriplesLoaded(count);
	// DataID.bean.pushDownloadInfo();
	// }
	// } catch (ArrayIndexOutOfBoundsException e) {
	// lastLine = u;
	// }
	// }
	// }
	//
	// } catch (NoSuchElementException em) {
	// // em.printStackTrace();
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	//
	// }
	// DataID.bean.setDownloadNumberOfTriplesLoaded(count);
	// DataID.bean
	// .setDownloadNumberOfDownloadedDistributions(DataID.bean
	// .getDownloadNumberOfDownloadedDistributions() + 1);
	//
	// DataID.bean.pushDownloadInfo();
	// object.close();
	// subject.close();
	// } catch (Exception e) {
	// System.out.println(e.getMessage());
	// }
	//
	// }
	//
	// };

	private boolean checkWheterDownload(String uri, String httpContentLength,
			String httpLastModified) {
		try {
			DistributionMongoDBObject distribution = new DistributionMongoDBObject(
					uri);
			if (distribution.getHttpByteSize().equals(httpContentLength)
					&& distribution.getHttpLastModified().equals(
							httpLastModified))
				return false;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

}