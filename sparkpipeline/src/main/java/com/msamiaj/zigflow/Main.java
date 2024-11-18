package com.msamiaj.zigflow;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.msamiaj.zigflow.ingestion.Ingestion;
import com.msamiaj.zigflow.preprocessing.Preprocessing;
import com.msamiaj.zigflow.utils.OutputDirConfig;
import com.msamiaj.zigflow.utils.Settings;

/**
 * 1: Analysis.
 * 2: SparkConf and SparkContext and what's is used.
 * 3: Caching.
 * 4: Deployment.
 * 5:
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static Ingestion ingestion;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("zigflow").master("local[*]").getOrCreate();
        SparkConf conf = spark.sparkContext().conf();
        conf.set("spark.driver.memory", "3g");
        conf.set("spark.executer.memory", "3g");

        spark.sparkContext().setCheckpointDir(Settings.checkpointPath);

        ingestion = new Ingestion(spark);
        ingestion.ingestCombinedDataFiles().ingestMovieTitlesFile();

        // Dataframes that have not been parsed or transformed yet!
        Dataset<Row> combinedDatasetUnionRaw = ingestion.getCombinedDatasetUnion();
        Dataset<Row> movieTitlesDatasetRaw = ingestion.getMovieTitlesDataset();

        combinedDatasetUnionRaw.show(10, false);
        combinedDatasetUnionRaw.printSchema();

        movieTitlesDatasetRaw.show(10, false);
        movieTitlesDatasetRaw.printSchema();

        // // Dataframes are now tansfomed into structured form.
        Dataset<Row> combinedDatasetUnionParsed = Preprocessing.processCombinedDataset(combinedDatasetUnionRaw);
        Dataset<Row> movieTitlesDatasetParsed = Preprocessing.processMovieTitlesDataset(movieTitlesDatasetRaw);

        logger.info("***Combined dataset union that has been parsed***.");

        // combinedDatasetUnionParsed.repartition(20);
        // combinedDatasetUnionParsed.persist(StorageLevel.MEMORY_ONLY_SER());
        combinedDatasetUnionParsed.show(80, false);
        combinedDatasetUnionParsed.printSchema();

        logger.info("***Movie titles dataset that have been parsed***.");

        // movieTitlesDatasetParsed.cache();
        movieTitlesDatasetParsed.show(80, false);
        movieTitlesDatasetParsed.printSchema();

        Dataset<Row> combinedDatasetDescStats = combinedDatasetUnionParsed.describe("Rating");
        // // Dataset<Row> ratingDistribution =
        // // combinedDatasetUnionParsed.groupBy("Rating").count();

        // logger.info("***Describing combinedDatasetUnionParsed on Rating column***");
        // // combinedDatasetDescStats.persist(StorageLevel.DISK_ONLY());
        combinedDatasetDescStats.show();

        // // logger.info("***Describing rating distributions***");
        // // ratingDistribution.persist(StorageLevel.DISK_ONLY());
        // // ratingDistribution.show();

        logger.info("***Writing datasets for visualization***");
        // ratingDistribution.coalesce(1).write().csv(Settings.outputPath +
        // "/ratingsDistribution");
        combinedDatasetDescStats.coalesce(1).write().csv(Settings.outputPath + "/combinedDatasetsDescStats");

        // Dataset<Row> joinedDataset = Preprocessing.performLargeJoin(
        // combinedDatasetUnionParsed,
        // movieTitlesDatasetParsed);

        // logger.info("***No of partitions for joinedDataset***: " +
        // joinedDataset.rdd().getNumPartitions());

        // // joinedDataset.rdd().checkpoint();
        // // joinedDataset.cache();

        // joinedDataset.show(10, false);

        // OutputDirConfig.renameOutputfiles("combinedDatasetsDescStats", "csv",
        // "combined_dataset_desc_stats.csv");
        // OutputDirConfig.renameOutputfiles("ratingsDistribution", "csv",
        // "rating_distribution.csv");

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
