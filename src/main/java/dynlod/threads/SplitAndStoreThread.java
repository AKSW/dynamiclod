package dynlod.threads;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.RDFHandlerBase;

import dynlod.DynlodGeneralProperties;

public class SplitAndStoreThread extends RDFHandlerBase {

	final static Logger logger = Logger.getLogger(SplitAndStoreThread.class);

	private String fileName;

	private boolean doneReadingFile = false;

	ConcurrentLinkedQueue<String> objectQueue = null;

	ConcurrentLinkedQueue<String> subjectQueue = null;

	public Integer subjectLines = 0;

	public Integer objectLines = 0;

	public Integer totalTriples = 0;
	
	public Integer totalTriplesRead = 0;
	
	private String lastSubject = "";

	public boolean isChain = true;

	private int bufferSize = 100000;

	BufferedWriter subjectFile = null;

	BufferedWriter objectFile = null;

	public SplitAndStoreThread(ConcurrentLinkedQueue<String> subjectQueue,
			ConcurrentLinkedQueue<String> objectQueue, String fileName) {
		this.objectQueue = objectQueue;
		this.subjectQueue = subjectQueue;
		this.fileName = fileName;
		startQueues();

	}

	public SplitAndStoreThread(ConcurrentLinkedQueue<String> subjectQueue,
			ConcurrentLinkedQueue<String> objectQueue, String fileName,
			boolean isChain) {
		this.objectQueue = objectQueue;
		this.subjectQueue = subjectQueue;
		this.fileName = fileName;
		this.isChain = isChain;
		startQueues();

	}

	private void startQueues() {
		try {
			if (subjectQueue != null)
				subjectFile = new BufferedWriter(new FileWriter(
						DynlodGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH
								+ fileName));
			if (objectQueue != null)
				objectFile = new BufferedWriter(new FileWriter(
						DynlodGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH
								+ fileName));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void closeQueues() {
		try {
			if (objectFile != null)
				objectFile.close();
			if (subjectFile != null)
				subjectFile.close();
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

	public void saveStatement(String stSubject, String stPredicate,
			String stObject) {

		try {
			if (!stObject.equals("http://www.w3.org/2002/07/owl#Class")
					&& !stPredicate
							.equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {
				
				
				// compare the current subject with the previous one
				if(!stSubject.equals(lastSubject)){
					
					// get subject and save to file
					subjectFile.write(stSubject+"\n");
					subjectLines++;
					lastSubject = stSubject;
					if (isChain)
						subjectQueue.add(stSubject);
					
				}

				// get object (make sure that its a resource and not a literal), add
				// to queue and save to file
				if (!stObject.startsWith("\"")) {
					String object;
					if(stObject.startsWith("<"))
					object = stObject.substring(1, stObject.length() -1);
					else 
						object = stObject;
					objectFile.write(object+"\n");

					// add object to object queue (the queue is read by another thread)
					if (isChain)
						objectQueue.add(object);
					objectLines++;
				}
				totalTriples++;
			}
			while (objectQueue.size() > bufferSize) {
				Thread.sleep(1);
			}
			while (subjectQueue.size() > bufferSize) {
				Thread.sleep(1);
				System.out.println(subjectQueue.size());
			}
			
			if (totalTriplesRead % 1000000 == 0) {
				logger.info("Triples read: " + totalTriplesRead);
				// System.out.println(objectQueue.size());
				// System.out.println(subjectQueue.size());
			}
			totalTriplesRead++;

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void handleStatement(Statement st) {
		String stSubject = st.getSubject().toString();
		String stPredicate = st.getPredicate().toString();
		String stObject = st.getObject().toString();
		saveStatement(stSubject, stPredicate, stObject);
	}
}
