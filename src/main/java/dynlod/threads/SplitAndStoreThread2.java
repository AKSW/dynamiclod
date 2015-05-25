package dynlod.threads;

import java.io.FileOutputStream;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.RDFHandlerBase;

import dynlod.DynlodGeneralProperties;

public class SplitAndStoreThread2 extends RDFHandlerBase {

	private String fileName;

	private boolean doneReadingFile = false;

	Queue<String> objectQueue = null;

	Queue<String> subjectQueue = null;

	public Integer subjectLines = 0;

	public Integer objectLines = 0;

	public Integer totalTriples = 0;

	public boolean isChain = true;

	private String tmpLastSubject = "";

	FileOutputStream subject = null;

	FileOutputStream object = null;

	public SplitAndStoreThread2(Queue<String> subjectQueue,
			Queue<String> objectQueue, String fileName) {
		this.objectQueue = objectQueue;
		this.subjectQueue = subjectQueue;
		this.fileName = fileName;
		startQueues();

	}

	public SplitAndStoreThread2(Queue<String> subjectQueue,
			Queue<String> objectQueue, String fileName, boolean isChain) {
		this.objectQueue = objectQueue;
		this.subjectQueue = subjectQueue;
		this.fileName = fileName;
		this.isChain = isChain;
		startQueues();

	}

	private void startQueues() {
		try {
			if (subjectQueue != null) {
				// creates subject file in disk
				subject = new FileOutputStream(
						DynlodGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH
								+ fileName);
			}
			if (objectQueue != null)
				// creates object file in disk
				object = new FileOutputStream(
						DynlodGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH
								+ fileName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void closeQueues() {
		try {
			if (object != null)
				object.close();
			if (subject != null)
				subject.close();
			// DataIDBean.pushDownloadInfo();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public String getFileName() {
		return fileName;
	}

	public void setDoneReadingFile(boolean doneReadingFile) {
		this.doneReadingFile = doneReadingFile;
	}

	public boolean isDoneReadingFile() {
		return doneReadingFile;
	}

	public Integer getSubjectLines() {
		return subjectLines;
	}

	public Integer getObjectLines() {
		return objectLines;
	}

	public Integer getTotalTriples() {
		return totalTriples;
	}

	@Override
	public void handleStatement(Statement st) {

		String stSubject = st.getSubject().toString();
		String stPredicate = st.getPredicate().toString();
		String stObject = st.getObject().toString();

		try {
			if (!stObject.equals("http://www.w3.org/2002/07/owl#Class")
					&& !stPredicate
							.equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {

				// get subject and save to file
				if (!tmpLastSubject.equals(stSubject)) {
					tmpLastSubject = stSubject;
					subject.write(new String(stSubject + "\n").getBytes());
					while (subjectQueue.size() > 1000) {
						Thread.sleep(1);
					}
					if (isChain)
						subjectQueue.add(stSubject);
					subjectLines++;
				}

				// get object (make sure that its a
				// resource and not a literal), add
				// to queue and save to file
				if (object != null)
					if (!stObject.startsWith("\"")) {
						object.write(new String(stObject + "\n").getBytes());

						// add object to object queue
						// (the queue is read by other
						// thread)
						while (objectQueue.size() > 1000) {
							Thread.sleep(1);
						}
						if (isChain)
							objectQueue.add(stObject);
						objectLines++;
					}
				totalTriples++;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
