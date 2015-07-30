package dynlod.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import dynlod.DynlodGeneralProperties;
import dynlod.mongodb.objects.DatasetMongoDBObject;
import dynlod.mongodb.objects.DistributionMongoDBObject;
import dynlod.mongodb.objects.DistributionObjectDomainsMongoDBObject;
import dynlod.mongodb.objects.DistributionSubjectDomainsMongoDBObject;
import dynlod.mongodb.objects.LinksetMongoDBObject;
import dynlod.mongodb.queries.Queries;

public class RemoveData extends HttpServlet {
	
	PrintWriter out;

	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	private void manageRequest(HttpServletRequest request,
			HttpServletResponse response) throws IOException {
		
		out = response.getWriter();

		Map<String, String[]> parameters = request.getParameterMap();

		if (parameters.containsKey("pass")) {
			if (parameters.get("pass")[0]
					.equals(DynlodGeneralProperties.REMOVE_DATASET_PASS)) {

				out.write("Pass ok. \n");
				
				if (parameters.containsKey("removeDataset")) {
					String dataset = parameters.get("removeDataset")[0].replace("_escaped_fragment","#");
					out.write("Removing: "+dataset+"\n");
					safelyRemoveDataset(dataset,
							dataset,
							dataset);
				}
			}
			else
				out.write("Pass error.\n");
		}
	}

	private void removeFromDomainList(String distributionURI) {
		ArrayList<String> domains = Queries.getMongoDBObject(
				DistributionObjectDomainsMongoDBObject.COLLECTION_NAME,
				DistributionObjectDomainsMongoDBObject.DISTRIBUTION_URI,
				distributionURI);
		for (String domain : domains) {
			DistributionObjectDomainsMongoDBObject dom = new DistributionObjectDomainsMongoDBObject(
					domain);
			dom.remove();
		}
		domains = Queries.getMongoDBObject(
				DistributionSubjectDomainsMongoDBObject.COLLECTION_NAME,
				DistributionObjectDomainsMongoDBObject.DISTRIBUTION_URI,
				distributionURI);
		for (String domain : domains) {
			DistributionSubjectDomainsMongoDBObject dom = new DistributionSubjectDomainsMongoDBObject(
					domain);
			dom.remove();
		}
	}

	private void removeLinksets(String distributionURI) {
		ArrayList<String> linksets = Queries.getMongoDBObject(
				LinksetMongoDBObject.COLLECTION_NAME,
				LinksetMongoDBObject.DISTRIBUTION_SOURCE,
				distributionURI);
		for (String linkset : linksets) {
			LinksetMongoDBObject dom = new LinksetMongoDBObject(linkset);
			dom.remove();
		}

		linksets = Queries.getMongoDBObject(
				LinksetMongoDBObject.COLLECTION_NAME,
				LinksetMongoDBObject.DISTRIBUTION_TARGET,
				distributionURI);
		for (String linkset : linksets) {
			LinksetMongoDBObject dom = new LinksetMongoDBObject(linkset);
			dom.remove();
		}
	}

	private void safelyRemoveDistribution(String distribution) {
		// case there is no parents, remove the domains, linksets, distributions
		// and the subset.
		removeFromDomainList(distribution);
		removeLinksets(distribution);
		DistributionMongoDBObject dist = new DistributionMongoDBObject(
				distribution);
		dist.remove();
	}

	private void safelyRemoveDataset(String dataset, String datasetToRemove,
			String lastDataset) {
		DatasetMongoDBObject d = new DatasetMongoDBObject(dataset);

		// check children and distributions
		List<String> children = d.getSubsetsURIs();
		for (String child : children) {
			safelyRemoveDataset(child, datasetToRemove, d.getUri());
		}

		List<String> distributions = d.getDistributionsURIs();
		for (String distribution : distributions) {
			DistributionMongoDBObject dist = new DistributionMongoDBObject(
					distribution);
			if (dist.getDefaultDatasets().size() > 1) {
				for (String defaultDataset : dist.getDefaultDatasets()) {
					if (defaultDataset.equals(dataset)) {
						dist.removeDefaultDataset(dataset);
						dist.updateObject(true);
					}
				}
			} else {
				safelyRemoveDistribution(dist.getUri());
			}
			d.removeSubsetURI(dist.getUri());
			d.updateObject(true);
		}

		if (d.getParentDatasetURI().size() == 1) {
			d.remove();
		} else {
			d.removeParentDatasetURI(lastDataset);
			d.updateObject(true);
		}

		if (dataset.equals(datasetToRemove)) {
			// check children and distributions
			List<String> parents = d.getParentDatasetURI();
			for (String parent : parents) {
				DatasetMongoDBObject dd = new DatasetMongoDBObject(parent);
				dd.removeSubsetURI(datasetToRemove);
				dd.updateObject(true);
			}
			d.remove();
		}

	}

}
