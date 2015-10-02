package dynlod.utils;

public class FQDN {

	public static String getFQDNFromString(String obj){
		String[] ar = obj.split("/");
		if (ar.length > 3)
			obj = ar[0] + "//" + ar[2] + "/" + ar[3] + "/";
		else if (ar.length > 2)
			obj = ar[0] + "//" + ar[2] + "/";
		else {
			obj = "";
		}
		
		return obj;
	}
	
}
