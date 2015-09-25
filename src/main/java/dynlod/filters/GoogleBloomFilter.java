package dynlod.filters;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.log4j.Logger;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

public class GoogleBloomFilter {
	
	final static Logger logger = Logger.getLogger(GoogleBloomFilter.class);

	public BloomFilter<byte[]> filter = null;

	private Funnel<byte[]> funnel = Funnels.byteArrayFunnel();

	public String fullFilePath;

	public GoogleBloomFilter(int insertions, double fpp) {
		if(fpp >1)
			fpp = 0.000000001;
		create(insertions, fpp);
	}

	public GoogleBloomFilter() {
		create(98, 0.01);
	}

	public boolean create(int insertions, double fpp) {
		if (filter == null)
			filter = BloomFilter.create(funnel, insertions, fpp);

		return true;
	}

	public boolean add(String s) {
		return filter.put(s.getBytes());

	}

	public boolean compare(String s) throws Exception {
		return filter.mightContain(s.getBytes());
	}

	public boolean saveFilter(String file) {

		try {
			filter.writeTo(new FileOutputStream(new File(file)));

		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.debug("Filter saved: "+file);
		
		return true;
	}

	public boolean loadFilter(String path, String distributionName) {
		try {
			filter = BloomFilter.readFrom(new FileInputStream(new File(path)),
					funnel);
			fullFilePath = path;
			
			logger.debug("Filter loaded from file: "+ distributionName);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			// DataID.bean.addDisplayMessage(DataIDGeneralProperties.MESSAGE_ERROR,e.getMessage());
			e.printStackTrace();
		}
		return true;
	}
}
