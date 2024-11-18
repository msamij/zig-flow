package com.msamiaj.zigflow;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.msamiaj.zigflow.ingestion.Ingestion;
import com.msamiaj.zigflow.preprocessing.Preprocessing;
import com.msamiaj.zigflow.utils.Settings;

/**
 * TODO: 1 Analysis.
 * 2: SparkConf and SparkContext and what's is used.
 * 3: Caching.
 * 4: Deployment.
 * 5:
 */
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

        logger.info("***Combined dataset union that has been parsed***.");

        // combinedDatasetUnionParsed.cache();
        combinedDatasetUnionParsed.show(80, false);
        combinedDatasetUnionParsed.printSchema();

        logger.info("***Movie titles dataset that have been parsed***.");

        // movieTitlesDatasetParsed.cache();
        movieTitlesDatasetParsed.show(80, false);
        movieTitlesDatasetParsed.printSchema();

        Dataset<Row> combinedDatasetDescStats = combinedDatasetUnionParsed.describe("Rating");

        logger.info("***Describing combinedDatasetUnionParsed on Rating column***");
        combinedDatasetDescStats.persist(StorageLevel.MEMORY_ONLY());
        combinedDatasetDescStats.show();

        logger.info("***Writing combined dataset desciptive statistics for visualization***");
        combinedDatasetDescStats.coalesce(1).write().csv(Settings.outputPath + "/combinedDatasetsDescStats");

        // Dataset<Row> joinedDataset = Preprocessing.performLargeJoin(
        // combinedDatasetUnionParsed,
        // movieTitlesDatasetParsed);

        // logger.info("***No of partitions for joinedDataset***: " +
        // joinedDataset.rdd().getNumPartitions());

        // joinedDataset.rdd().checkpoint();
        // joinedDataset.cache();

        // joinedDataset.show(10, false);

        File dir = new File(Settings.outputPath + "/combinedDatasetsDescStats");
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                if (file.getName().endsWith(".csv")) {
                    file.renameTo(new File(Settings.outputPath + "/combined_dataset_desc_stats.csv"));
                    break;
                }
            }
            // new File(dir.getParent() + "/combinedDatasetsDescStats").delete();
        }

        // Option<String> checkPointFile = joinedDataset.rdd().getCheckpointFile();
        // if (checkPointFile.isDefined()) {
        // logger.info("***Checkpoint is defined at***: " + checkPointFile.get());
        // }

        // joinedDataset.printSchema();
        // logger.info("***No of rows for joinedDataset***:" + joinedDataset.count());

        // joinedDataset
        // .filter("MovieID IS NULL OR CustomerID IS NULL OR Rating IS NULL OR Date IS
        // NULL OR YearOfRelease IS NULL OR Title IS NULL")
        // .show();
    }
}
