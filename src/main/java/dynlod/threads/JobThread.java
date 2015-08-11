package dynlod.threads;

import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class JobThread implements Runnable {
	int size;
	ArrayList<String> lines;
	DataModelThread dataThread = null;
	public ConcurrentHashMap<String, Integer> c;

	public JobThread(DataModelThread dataThread, ArrayList<String> lines, int size, ConcurrentHashMap<String, Integer> c) {
		this.c=c;
		this.size = size;
		this.lines = lines;
		this.dataThread = dataThread;

	}

	public void run() {
		try {
			for (String val: lines) {

				if (dataThread.filter.compare(val)) {
					dataThread.links.addAndGet(1);
					dataThread.availabilityCounter++;
					
					dataThread.weightCount++;
					dataThread.count++;
					
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
