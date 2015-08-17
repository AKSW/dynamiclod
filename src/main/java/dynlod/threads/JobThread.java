package dynlod.threads;

import java.util.ArrayList;

public class JobThread implements Runnable {
	ArrayList<String> lines;
	DataModelThread dataThread = null;

	public JobThread(DataModelThread dataThread, ArrayList<String> lines) {
		this.lines = lines;
		this.dataThread = dataThread;

	}

	public void run() {
		try {
			for (String val: lines) {

				if (dataThread.filter.compare(val)) {
					dataThread.links.addAndGet(1);
//					System.out.println(val + dataThread.links.get() + dataThread.sourceColumnIsSubject);
//					dataThread.availabilityCounter++;
//					
//					dataThread.weightCount++;
//					dataThread.count++;
//					
//					if(dataThread.count%dataThread.setSize==0)
//						dataThread.weight++;
//					
//					if(dataThread.weightCount%dataThread.weight==0){
//						String url = lines[i].replace("<", "").replace(">", "");
//						
//						if(c.putIfAbsent(url, -1) == null){
////							new ResourceAvailability((dataThread.count%dataThread.setSize), url, 2000, dataThread.urlStatus, c);
//							dataThread.listURLToTest.put(dataThread.count%dataThread.setSize, url);
//						}
////						else
////							dataThread.urlStatus.put((dataThread.count%dataThread.setSize), 
////									new ResourceInstance(url, c.putIfAbsent(url, 0)));
//						dataThread.weightCount = 0;						
//					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
