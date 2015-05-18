package dynlod.linksets;

import javax.servlet.http.HttpServlet;

import dynlod.mongodb.objects.SystemPropertiesMongoDBObject;

public class LinksetTimer extends HttpServlet implements Runnable {

	public void init() {
		(new Thread(new LinksetTimer())).start();

	}

	public void run() {

		while (true) {
			SystemPropertiesMongoDBObject systemProperties = null;

			systemProperties = new SystemPropertiesMongoDBObject();
			
			if(systemProperties.getLinksetNeedUpdate()==null){
				systemProperties.setLinksetNeedUpdate(true);
				systemProperties.updateObject(true);
			}
			

			if (systemProperties.getLinksetNeedUpdate()==true) {
				systemProperties.setLinksetNeedUpdate(false);
				systemProperties.updateObject(true);
				MakeLinksets m = new MakeLinksets();
				m.updateLinksets();
			}

			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}