package com.msamiaj.zigflow.ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.msamiaj.zigflow.utils.Settings;

public class Ingestion {
	private SparkSession spark;

	private Dataset<Row> combinedData1;
	private Dataset<Row> combinedData2;
	private Dataset<Row> combinedData3;
	private Dataset<Row> combinedData4;
	private Dataset<Row> movieTitlesDataset;
	private Dataset<Row> combinedDatasetUnion;

	public Ingestion(final SparkSession spark) {
		this.spark = spark;
	}

	private Dataset<Row> loadDataset(String filePath, String fileType) {
		return spark.read().format(fileType).load(filePath);
	}

	public Ingestion ingestCombinedDataFiles() {
		combinedData1 = loadDataset(Settings.datasetsPath + "/combined_data_1.txt", "text");
		combinedData2 = loadDataset(Settings.datasetsPath + "/combined_data_2.txt", "text");
		combinedData3 = loadDataset(Settings.datasetsPath + "/combined_data_3.txt", "text");
		combinedData4 = loadDataset(Settings.datasetsPath + "/combined_data_4.txt", "text");

		combinedDatasetUnion = combinedData1
				.unionByName(combinedData2, false)
				.unionByName(combinedData3, false)
				.unionByName(combinedData4, false);
		return this;
	}

	public Ingestion ingestMovieTitlesFile() {
		movieTitlesDataset = spark.read()
				.option("inferSchema", "true")
				.option("header", "false")
				.option("delimiter", "\t")
				.option("multiline", "true")
				.csv(Settings.datasetsPath + "/movie_titles.csv");
		return this;
	}

	public Dataset<Row> getMovieTitlesDataset() {
		return movieTitlesDataset != null ? movieTitlesDataset : null;
	}

	public Dataset<Row> getCombinedDatasetUnion() {
		return combinedDatasetUnion != null ? combinedDatasetUnion : null;
	}
}
