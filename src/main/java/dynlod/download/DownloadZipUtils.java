package dynlod.download;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.log4j.Logger;
import org.junit.Test;

import dynlod.exceptions.DynamicLODGeneralException;
import dynlod.utils.FileUtils;

public class DownloadZipUtils {
	
	final static Logger logger = Logger.getLogger(DownloadZipUtils.class);

	static final int BUFFER = 512;

	public boolean checkCompressedDistribution() {

		return false;
	}

	public void checkZipFile(URL url) throws DynamicLODGeneralException {
		ZipInputStream zis = null;
		try {
			try {
			HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
			InputStream inputStream = httpConn.getInputStream();
		 
		   zis = new ZipInputStream(new BufferedInputStream(inputStream));
			ZipEntry entry = null;
			int count2 = 1;
			String fileName = null;
			
			logger.info("Testing ZIP file...");

				while ((entry = zis.getNextEntry()) != null) {
					fileName = entry.getName();
					if (count2 > 1) {
						throw new DynamicLODGeneralException(
								"Too many entries compressed! ZIP files should contains only the dump file.");
					}
					if (entry.isDirectory()) {
						throw new DynamicLODGeneralException(
								"We found a compressed directory ("
										+ entry.getName()
										+ "). ZIP files should contains only the dump file.");
					}
					if (!FileUtils.acceptedFormats(entry.getName())) {
						throw new DynamicLODGeneralException(
								"The file format is invalid. "
										+ entry.getName());
					}
					count2++;
				}
				
				logger.info("ZIP file is good to go.");
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		} finally {
			try {
				zis.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
}
