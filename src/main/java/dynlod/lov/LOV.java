package dynlod.lov;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.junit.Test;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.core.DatasetGraph;

import dynlod.DynlodGeneralProperties;
import dynlod.download.Download;
import dynlod.filters.FileToFilter;
import dynlod.filters.GoogleBloomFilter;
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.DistributionSubjectDomainsMongoDBObject;
import dynlod.utils.FileUtils;
import dynlod.utils.Timer;

public class LOV extends Download {
	final static Logger logger = Logger.getLogger(LOV.class);

	DistributionMongoDBObject dist = null;

	@Test
	public void loadLOVVocabularies() throws Exception {
		
		logger.info("Loading LOV vocabulary.");;

		
		Model m = ModelFactory.createDefaultModel();
		Model tmpModel = ModelFactory.createDefaultModel();
		
		new DynlodGeneralProperties().loadProperties();

		setUrl(new URL(DynlodGeneralProperties.LOV_URL));
//		setUrl(new URL("http://data.pokepedia.fr/dumps/pokepedia-fr_rdfdump.tar.gz"));
//		
		// download lov file
		openStream();

		// allowing gzip format
		checkGZipInputStream();
		
//		inputStream = getTarInputStream(inputStream);
		

		simpleDownload(DynlodGeneralProperties.BASE_PATH + "lov.tmp",
				inputStream);

		
		DatasetGraph dg = RDFDataMgr.loadDatasetGraph(
				DynlodGeneralProperties.BASE_PATH + "lov.tmp", Lang.NQUADS);


		Iterator<Node> tmpNodeIt = dg.listGraphNodes();
		
		int a = 0;
		
		Node tmpNode =null;
		while (tmpNodeIt.hasNext()) {
			tmpNode = tmpNodeIt.next();
			Graph tmpGraph = dg.getGraph(tmpNode);			
			
			tmpModel = ModelFactory.createModelForGraph(tmpGraph);
		
			if(tmpNode.getURI().equals("http://lov.okfn.org/dataset/lov")){
				System.out.println("");
				break;
			}
		
		}

		
		
		Iterator<Node> nodeIt = dg.listGraphNodes();
		
		
		int i = 0;

		while (nodeIt.hasNext()) {
			Node node = nodeIt.next();
			Graph graph = dg.getGraph(node);			
			
			m = ModelFactory.createModelForGraph(graph);

			Property p = ResourceFactory
					.createProperty("http://purl.org/dc/terms/title");

			Property p2 = ResourceFactory
					.createProperty("http://www.w3.org/2000/01/rdf-schema#label");

			Resource r = ResourceFactory
					.createResource(node.getURI());
			
			System.out.println(node.getURI());
			System.out.println(tmpNode.getURI());

			// System.out.println(node.getNameSpace()+" "+i++);

			// new dataset at mongodb
			DatasetMongoDBObject d = new DatasetMongoDBObject(
					node.getNameSpace());
			StmtIterator stmt = tmpModel.listStatements(r, p, (RDFNode) null);
			
			if (stmt.hasNext())
				d.setTitle(stmt.next().getObject().toString());
			

			stmt = tmpModel.listStatements(r, p2, (RDFNode) null);
			if (stmt.hasNext())
				d.setLabel(stmt.next().getObject().toString());
			
			d.setIsVocabulary(true);

			d.updateObject(true);

			StmtIterator triples = m.listStatements(null, null, (RDFNode) null);

			ArrayList<String> subjects = new ArrayList<String>();
			ArrayList<String> objects = new ArrayList<String>();

			while (triples.hasNext()) {

				Statement triple = triples.next();

				subjects.add("<" + triple.getSubject().toString() + ">");
				if (triple.getObject().isResource())
					objects.add("<" + triple.getObject().toString() + ">");

			}
			dist = new DistributionMongoDBObject(node.getNameSpace());
			if (d.getTitle() != null)
				dist.setTitle(d.getTitle());
			else if (d.getLabel() != null)
				dist.setTitle(d.getLabel());

			SaveDist(node.getNameSpace(), subjects, objects);

		}

	}

	public void SaveDist(String nameSpace, ArrayList<String> subjects,
			ArrayList<String> objects) throws Exception {
		File fout = new File(
				DynlodGeneralProperties.SUBJECT_FILE_DISTRIBUTION_PATH
						+ FileUtils.stringToHash(nameSpace));
		FileOutputStream fos = new FileOutputStream(fout);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		for (String string : subjects) {
			bw.write(string);
			bw.newLine();
		}

		bw.close();

		fout = new File(DynlodGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH
				+ FileUtils.stringToHash(nameSpace));
		fos = new FileOutputStream(fout);
		bw = new BufferedWriter(new OutputStreamWriter(fos));

		for (String string : objects) {
			bw.write(string);
			bw.newLine();
		}
		bw.close();

		String obj = nameSpace;
		String[] ar = obj.split("/");
		if (ar.length > 3)
			obj = ar[0] + "//" + ar[2] + "/" + ar[3] + "/";
		else if (ar.length > 2)
			obj = ar[0] + "//" + ar[2] + "/";
		else {
			obj = "";
		}

		Timer t = new Timer();
		t.startTimer();

		// make a filter with subjects
		GoogleBloomFilter filter;
		if (subjects.size() > 1000000)
			filter = new GoogleBloomFilter((int) subjects.size(),
					(0.9) / subjects.size());
		else
			filter = new GoogleBloomFilter((int) subjects.size(), 0.0000001);

		// load file to filter and take the process time
		FileToFilter f = new FileToFilter();

		// Loading file to filter
		f.loadFileToFilter(filter, FileUtils.stringToHash(nameSpace));

		filter.saveFilter(FileUtils.stringToHash(nameSpace));
		// save filter
		String timer = t.stopTimer();

		ArrayList<String> parentDataset = new ArrayList<String>();
		parentDataset.add(nameSpace);

		dist.setDownloadUrl(nameSpace);
		dist.setDefaultDatasets(parentDataset);
		dist.setParentDataset(nameSpace);
		dist.setTopDataset(nameSpace);
		dist.setTriples(subjects.size() + objects.size());
		dist.setTimeToCreateFilter(timer);
		dist.setFormat("nq");
		dist.setIsVocabulary(true);
		dist.setNumberOfObjectTriples(String.valueOf(objects.size()));
		dist.setNumberOfTriplesLoadedIntoFilter(String.valueOf(subjects.size()));
		dist.setSuccessfullyDownloaded(true);
		dist.setStatus(DistributionMongoDBObject.STATUS_WAITING_TO_CREATE_LINKSETS);
		dist.setSubjectFilterPath(DynlodGeneralProperties.SUBJECT_FILE_FILTER_PATH
				+ FileUtils.stringToHash(nameSpace));
		dist.setObjectPath(DynlodGeneralProperties.OBJECT_FILE_DISTRIBUTION_PATH
				+ FileUtils.stringToHash(nameSpace));

		dist.updateObject(true);

		ObjectId id = new ObjectId();
		DistributionSubjectDomainsMongoDBObject ds = new DistributionSubjectDomainsMongoDBObject(
				id.get().toString());
		ds.setDistributionURI(nameSpace);
		ds.setSubjectDomain(obj);
		ds.updateObject(true);
	}

}
