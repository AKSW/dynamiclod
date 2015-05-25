package dynlod.filters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import dynlod.DynlodGeneralProperties;

public class FileToFilter {
	
	final static Logger logger = Logger.getLogger(FileToFilter.class);

	// list of linksets
	List<String> links = new ArrayList<String>();
	public List<String> linksUniq = new ArrayList<String>();
	public int subjectsLoadedIntoFilter = 0;

	public void loadFileToFilter(GoogleBloomFilter filter, String fileName) {

		BufferedReader br = null;
		
		logger.info("Loading file to bloom filter: "+ 
						DynlodGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH+fileName);
		
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(DynlodGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH+
					fileName));
			String oi = null;
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.replace("<", "");
				sCurrentLine = sCurrentLine.replace(">", "");
				filter.add(sCurrentLine);
				subjectsLoadedIntoFilter++;
				oi = sCurrentLine;
			}
			
			logger.info("Bloom filter loaded "+subjectsLoadedIntoFilter + " lines.");
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				
			}
		}
		
		try{
//			File f = new File(DynlodGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH+
//							fileName);
//			f.delete();
//			
//			logger.debug("deleting "+ DynlodGeneralProperties.BASE_PATH+
//					fileName);
//			f = new File(DynlodGeneralProperties.BASE_PATH+
//					fileName);
//			
//			f.delete();
	
		}
		catch(Exception e){
			e.printStackTrace();
//			bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_ERROR,e.getMessage());
		}
	

	}

//	public int searchFileOnFilter(GoogleBloomFilter filter, String path) throws Exception {
//		BufferedReader br = null;
//		try {
//
//			int ie = 0;
//			String sCurrentLine;
//			br = new BufferedReader(new FileReader(path));
//			
//			while ((sCurrentLine = br.readLine()) != null) {
//				if (filter.compare(sCurrentLine)) { 
//					links.add(sCurrentLine);
//				}
//			}
//			
//			// remove duplicates from the linkset list
//			Iterator<String> i = links.iterator();
//			
//			while(i.hasNext()){
//				String link = i.next();
//				if(!linksUniq.contains(link)){
//					linksUniq.add(link);
//				}
//			}
//
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if (br != null)
//					br.close();
//			} catch (IOException ex) {
//				ex.printStackTrace();
//			}
//		}
//
//		return linksUniq.size();
//	}

}
