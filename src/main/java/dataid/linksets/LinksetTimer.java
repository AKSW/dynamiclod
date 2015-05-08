package dataid.linksets;

import org.junit.Test;

import dataid.mongodb.objects.SystemPropertiesMongoDBObject;


public class LinksetTimer {

	@Test
	public void LinksetTimer() {
		while (true) {
			SystemPropertiesMongoDBObject systemProperties = new SystemPropertiesMongoDBObject();

			if (systemProperties.getLinksetNeedUpdate()) {
				systemProperties.setLinksetNeedUpdate(false);
				systemProperties.updateObject(true);
				MakeLinksets m = new MakeLinksets();
				m.updateLinksets();
			}

			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
