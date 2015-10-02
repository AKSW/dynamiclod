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
	public int elementsLoadedIntoFilter;

	public void loadFileToFilter(GoogleBloomFilter filter, String file) {

		BufferedReader br = null;
		elementsLoadedIntoFilter = 0;
		
		logger.info("Loading file to bloom filter: "+file);
		
		try {
			String sCurrentLine;
			String sLastLine = "";
			br = new BufferedReader(new FileReader(file));

			while ((sCurrentLine = br.readLine()) != null) {
				if(!sLastLine.equals(sCurrentLine)){
					sCurrentLine = sCurrentLine.replace("<", "");
					sCurrentLine = sCurrentLine.replace(">", "");
					filter.add(sCurrentLine);
					elementsLoadedIntoFilter++;
				}
				sLastLine = sCurrentLine;
			}
			
			logger.info("Bloom filter loaded "+elementsLoadedIntoFilter + " elements.");
			
			
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


}
