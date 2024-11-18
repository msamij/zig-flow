package com.msamiaj.zigflow.ingestion;

import java.nio.file.Paths;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.msamiaj.zigflow.utils.Settings;

public class Ingestion {
	private final SparkSession spark;

	public Ingestion(final SparkSession spark) {
		this.spark = spark;
	}

	private Dataset<Row> loadCombinedDataset(String filePath, String fileExt) {
		return spark.read().format(fileExt).load(filePath);
	}

	public Dataset<Row> ingestCombinedDataFiles() {
		Dataset<Row> combinedData1 = loadCombinedDataset(
				Paths.get(Settings.datasetsPath).resolve("combined_data_1.txt").toString(), "text");

		Dataset<Row> combinedData2 = loadCombinedDataset(
				Paths.get(Settings.datasetsPath).resolve("combined_data_2.txt").toString(), "text");

		Dataset<Row> combinedData3 = loadCombinedDataset(
				Paths.get(Settings.datasetsPath).resolve("combined_data_3.txt").toString(), "text");

		Dataset<Row> combinedData4 = loadCombinedDataset(
				Paths.get(Settings.datasetsPath).resolve("combined_data_4.txt").toString(), "text");

		return combinedData1.unionByName(combinedData2, false)
				.unionByName(combinedData3, false)
				.unionByName(combinedData4, false);

	}

	public Dataset<Row> ingestMovieTitlesFile() {
		return spark.read()
				.option("inferSchema", "true")
				.option("header", "false")
				.option("delimiter", "\t")
				.option("multiline", "true")
				.csv(Paths.get(Settings.datasetsPath).resolve("movie_titles.csv").toString());
	}
}
