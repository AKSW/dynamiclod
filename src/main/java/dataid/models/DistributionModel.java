package dataid.models;

public class DistributionModel {

	int id;

	String subsetURI;
	
	String datasetURI;

	String distributionURI;
	
	String distriutionDownloadURL;
	

	public DistributionModel(int id, String dataset, String subset, String distributionURI, String distriutionDownloadURL) {
		this.id = id;
		this.subsetURI = subset;
		this.distributionURI = distributionURI;
		this.datasetURI = dataset;
		this.distriutionDownloadURL = distriutionDownloadURL;
	}
	
	public String getDatasetURI() {
		return datasetURI;
	}
	
	public void setDatasetURI(String datasetURI) {
		this.datasetURI = datasetURI;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getSubsetURI() {
		return subsetURI;
	}

	public void setSubsetURI(String subset) {
		this.subsetURI = subset;
	}

	public String getDistribution() {
		return distributionURI;
	}

	public void setDistribution(String distribution) {
		this.distributionURI = distribution;
	}

	public String getDistributionURI() {
		return distributionURI;
	}

	public void setDistributionURI(String distributionURI) {
		this.distributionURI = distributionURI;
	}

	public String getDistriutionDownloadURL() {
		return distriutionDownloadURL;
	}

	public void setDistriutionDownloadURL(String distriutionDownloadURL) {
		this.distriutionDownloadURL = distriutionDownloadURL;
	}
	
	

}
