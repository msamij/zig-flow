package com.msamiaj.zigflow;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.msamiaj.zigflow.ingestion.Ingestion;
import com.msamiaj.zigflow.preprocessing.Preprocessing;
import com.msamiaj.zigflow.utils.Settings;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static Ingestion ingestion;

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("zigflow").master("local").getOrCreate();
        spark.sparkContext().setCheckpointDir(Settings.checkpointPath);

        ingestion = new Ingestion(spark);
        ingestion.ingestCombinedDataFiles().ingestMovieTitlesFile();

        Dataset<Row> combinedDatasetUnion = ingestion.getCombinedDatasetUnion();
        Dataset<Row> movieTitlesDataset = ingestion.getMovieTitlesDataset();

        combinedDatasetUnion.show(10, false);
        combinedDatasetUnion.printSchema();

        movieTitlesDataset.show(10, false);
        movieTitlesDataset.printSchema();

        Dataset<Row> combinedDatasetUnionParsed = Preprocessing.processCombinedDataset(combinedDatasetUnion);
        Dataset<Row> movieTitlesDatasetParsed = Preprocessing.processMovieTitlesDataset(movieTitlesDataset);

        logger.info("***Combined dataset union that have been parsed.");
        combinedDatasetUnionParsed.show(80, false);
        combinedDatasetUnionParsed.printSchema();

        logger.info("***Movie titles dataset that have been parsed.");
        movieTitlesDatasetParsed.show(80, false);
        movieTitlesDatasetParsed.printSchema();

        Dataset<Row> joinedDataset = Preprocessing.performLargeJoin(
                combinedDatasetUnionParsed,
                movieTitlesDatasetParsed);

        int noOfPartitions = joinedDataset.rdd().getNumPartitions();
        logger.info("***No of partitions for joinedDataset***: " + noOfPartitions);

        // joinedDataset.persist(StorageLevel.MEMORY_AND_DISK());
        joinedDataset.show(10, false);

        joinedDataset.printSchema();
        joinedDataset.count();

        joinedDataset
                .filter("MovieID IS NULL OR CustomerID IS NULL OR Rating IS NULL OR Date IS NULL OR YearOfRelease IS NULL OR Title IS NULL")
                .show();
    }
}
