package dataid.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.junit.Test;

import dataid.Manager;
import dataid.mongodb.objects.DistributionMongoDBObject;
import dataid.mongodb.queries.DistributionQueries;

public class ReDownloadBadFiles  extends HttpServlet{
	final static Logger logger = Logger.getLogger(ReDownloadBadFiles.class);
	
	
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		ReDownloadBadFiles();
	}
	
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		ReDownloadBadFiles();
	}

	@Test
	public void ReDownloadBadFiles() {
		ArrayList<DistributionMongoDBObject> dist = DistributionQueries.getDistributionsWithErrors();
		List<DistributionMongoDBObject> distributionsLinks = new ArrayList<DistributionMongoDBObject>();
		
		logger.info("Searching for bad downloaded file.");
		
		for (DistributionMongoDBObject distributionMongoDBObject : dist) {
			distributionMongoDBObject.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_DOWNLOAD);
			distributionMongoDBObject.updateObject(true);
			distributionsLinks.add(distributionMongoDBObject);
		}
		if(distributionsLinks.size()>0){
		logger.info("Starting manager");
		
		new Manager(distributionsLinks);
		}
		else
			logger.info("No badly downloaded file found!");
		
	}
	
	
}
