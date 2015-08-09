package dynlod.files;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import dynlod.DynlodGeneralProperties;
import dynlod.utils.Formats;

@Deprecated
public class PrepareFiles {
	
	final static Logger logger = Logger.getLogger(PrepareFiles.class);
	
	public String subjectFile;
	public String objectFile;
	
	public int objectTriples;
	public int totalTriples;
	
//	public ArrayList<String> domains = new ArrayList<String>();
	public ConcurrentHashMap<String,Integer> objectDomains = new ConcurrentHashMap<String,Integer>();
	public ConcurrentHashMap<String,Integer> subjectDomains = new ConcurrentHashMap<String,Integer>();
	public ConcurrentLinkedQueue<String> results = new ConcurrentLinkedQueue<String>();

	public void separateSubjectAndObject(String fileName, String extension) throws Exception {
		
		String rapperFormat = null;
		
		if (Formats.getEquivalentFormat(extension).equals(Formats.DEFAULT_TURTLE)) rapperFormat = "turtle";
		else if (Formats.getEquivalentFormat(extension).equals(Formats.DEFAULT_RDFXML)) rapperFormat = "rdfxml";
		
//		if(extension.equals(Formats.DEFAULT_TURTLE) && isDbpedia) rapperFormat = "ntriples";
		
		
		// creates 2 files, one with subjects and other with objects
		logger.info("Creating subject and object files: "
				+ DynlodGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH
				+ fileName+" "+ DynlodGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH
				+ fileName);
		
		RunCommand r = new RunCommand();
		
		r.runRapper("rapper -w -i "+rapperFormat+" "+DynlodGeneralProperties.DUMP_PATH+ fileName+
				" -o ntriples | awk 'BEGIN{objcount=0;} {subjects=$1; objects=$3; if(lastlineSubjects!=subjects){ print subjects>\""+
				DynlodGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH+ fileName+
				"\"; print \"[subjectDomain]\"subjects; lastlineSubjects=subjects} if(objects~/^</){print objects>\""+
				DynlodGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH+ fileName+
				"\"; print \"[objectDomain]\"objects; objcount++}} END{print \"objectTriples \" objcount}'  | awk -F/ '{gsub(\">\",\"\",$0);print $1\"//\"$3\"/\"$4\"/\"}' | awk '{x[$0]++; if(x[$0]==50 || ($0 ~ \"objectTriples\" )){print $0} }'", subjectDomains, objectDomains);
	
		
		objectFile = DynlodGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH+ fileName;
		
		objectTriples = r.objectTriples;
		totalTriples = r.totalTriples;
		
	}
}
