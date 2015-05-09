package dataid.server;

import java.io.IOException;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import dataid.mongodb.objects.DatasetMongoDBObject;
import dataid.mongodb.objects.LinksetMongoDBObject;
import dataid.mongodb.queries.DatasetQueries;
import dataid.mongodb.queries.LinksetQueries;
import dataid.mongodb.queries.Queries;
import dataid.ontology.Dataset;
import dataid.ontology.NS;

public class CreateOutputByDatasets extends HttpServlet {

	private Model outModel =null;

	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		printOutput(response);
	}
	
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		printOutput(response);
	}
	
	public void printOutput(HttpServletResponse response){

		try {
			outModel = ModelFactory.createDefaultModel();

			outModel.setNsPrefix("rdfs", NS.RDFS_URI);
			outModel.setNsPrefix("dcat", NS.DCAT_URI);
			outModel.setNsPrefix("void", NS.VOID_URI);
			outModel.setNsPrefix("sd", NS.SD_URI);
			outModel.setNsPrefix("prov", NS.PROV_URI);
			outModel.setNsPrefix("dct", NS.DCT_URI);
			outModel.setNsPrefix("xsd", NS.XSD_URI);
			outModel.setNsPrefix("foaf", NS.FOAF_URI);
			outModel.setNsPrefix("dataid", NS.DATAID_URI);

			ArrayList<DatasetMongoDBObject> datasetList = DatasetQueries.getDatasets();

			if (datasetList != null)
				for (DatasetMongoDBObject dataset : datasetList) {
					Resource r = outModel.createResource(dataset.getUri());
					r.addProperty(
							Dataset.type,
							ResourceFactory.createResource(NS.VOID_URI
									+ "Dataset"));
					r.addProperty(
							Dataset.title,
							dataset.getTitle());
					r.addProperty(
							Dataset.label,
							dataset.getLabel());
				}

			ArrayList<LinksetMongoDBObject> linksetList = LinksetQueries
					.getLinksetsGroupByDatasets();

			if (linksetList != null)
				for (LinksetMongoDBObject linkset : linksetList) {
//					if(linkset.getLinks()>30)
					if (!linkset.getObjectsDatasetTarget().equals(
							linkset.getSubjectsDatasetTarget())) {
						Resource r = outModel.createResource(linkset.getUri());
						r.addProperty(
								Dataset.type,
								ResourceFactory.createResource(NS.VOID_URI
										+ "Linkset"));
						r.addProperty(
								ResourceFactory.createProperty(NS.VOID_URI
										+ "objectsTarget"), ResourceFactory
										.createProperty(linkset
												.getSubjectsDatasetTarget()
												.toString()));
						r.addProperty(ResourceFactory
								.createProperty(NS.VOID_URI + "subjectsTarget"),
								ResourceFactory.createProperty(linkset
										.getObjectsDatasetTarget().toString()));
						r.addProperty(ResourceFactory
								.createProperty(NS.VOID_URI + "triples"),
								ResourceFactory.createPlainLiteral(String.valueOf(linkset.getLinks())));
					}
				}

			if (linksetList.isEmpty() && datasetList.isEmpty())
				response.getWriter()
						.println(
								"There are no DataIDs files inserted! Please insert a DataID file and try again.");
			else {

				outModel.write(System.out, "TURTLE");
				outModel.write(response.getWriter(), "TURTLE");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
