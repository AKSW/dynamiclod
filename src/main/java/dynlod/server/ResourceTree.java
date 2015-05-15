package dynlod.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.junit.Test;

import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.queries.DatasetQueries;

public class ResourceTree extends HttpServlet {
	
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		getTree(request, response);
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		getTree(request, response);
	}


	ArrayList<DatasetMongoDBObject> d = new ArrayList<DatasetMongoDBObject>();

	public void getTree(HttpServletRequest request,
			HttpServletResponse response) {
		d = DatasetQueries.getDatasetsNotVocab();

		JsonObject core = new JsonObject();
		JsonObject data = new JsonObject();

		JsonArray datasetArray = new JsonArray();
		for (DatasetMongoDBObject dataset : d) {
			ArrayList<String> parent_list = dataset.getParentDatasetURI();
			for (String parent : parent_list) {
				JsonObject jsonparent = new JsonObject();
				jsonparent.put("parent", parent);
				jsonparent.put("id", dataset.getUri());
				jsonparent.put("text", dataset.getTitle());
				datasetArray.add(jsonparent);
				
				List<String> distribution_list = dataset.getDistributionsURIs();
				for (String distribution : distribution_list) {
					JsonObject jsondistribution = new JsonObject();
					DistributionMongoDBObject d = new DistributionMongoDBObject(distribution);
					jsondistribution.put("parent", parent);
					jsondistribution.put("id", d.getUri());
					jsondistribution.put("text", d.getTitle());
					datasetArray.add(jsondistribution);
					System.out.println(distribution);
				}
			}
			if (parent_list.size() == 0) {
				JsonObject jsonparent = new JsonObject();
				jsonparent.put("parent", "#");

				jsonparent.put("id", dataset.getUri());
				
				jsonparent.put("text", dataset.getTitle());
				datasetArray.add(jsonparent);
				
				List<String> distribution_list = dataset.getDistributionsURIs();
				for (String distribution : distribution_list) {
					JsonObject jsondistribution = new JsonObject();
					DistributionMongoDBObject d = new DistributionMongoDBObject(distribution);
					jsondistribution.put("parent", dataset.getUri());
					jsondistribution.put("id", d.getUri());
					jsondistribution.put("text", d.getTitle());
					datasetArray.add(jsondistribution);
					
				}
			}
			data.put("data", datasetArray);
			
		}
		core.put("core", data);
		try {
			response.getWriter().print(core);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
