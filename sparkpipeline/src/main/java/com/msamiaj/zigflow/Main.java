package com.msamiaj.zigflow;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.count;

import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.msamiaj.zigflow.ingestion.Ingestion;
import com.msamiaj.zigflow.preprocessing.Preprocessing;
import com.msamiaj.zigflow.utils.OutputDirConfig;
import com.msamiaj.zigflow.utils.Settings;

public class Main {
        private static Ingestion ingestion;
        private static final Logger logger = LoggerFactory.getLogger(Main.class);

        public static void main(String[] args) {
                SparkConf conf = new SparkConf()
                                .setAppName("zigflow")
                                .setMaster("local")
                                .set("spark.driver.memory", "2g")
                                .set("spark.executor.memory", "4g");

                SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
                spark.sparkContext().setCheckpointDir(Settings.checkpointPath);

                ingestion = new Ingestion(spark);

                // Dataframes that have not been parsed or transformed yet!
                Dataset<Row> combinedDatasetUnionRaw = ingestion.ingestCombinedDataFiles();
                Dataset<Row> movieTitlesDatasetRaw = ingestion.ingestMovieTitlesFile();

                // Spark now stores the DAG containing transformations for Dataframes!
                Dataset<Row> combinedDatasetUnionParsed = Preprocessing.processCombinedDataset(combinedDatasetUnionRaw);
                Dataset<Row> movieTitlesDatasetParsed = Preprocessing.processMovieTitlesDataset(movieTitlesDatasetRaw);

                // Get's average rating for each movie and rating count (i.e no of rating each
                // movie had received).
                Dataset<Row> aggAvgRatingCombinedDataset = combinedDatasetUnionParsed.groupBy("MovieID")
                                .agg(avg("Rating").alias("AvgRating"), count("Rating").alias("RatingCount"));

                // Get's the rating distribution, i.e how much count each rating value have.
                Dataset<Row> combinedDatasetRatingDistribution = combinedDatasetUnionParsed.groupBy("Rating").count();

                // Get the descriptive stats for Rating col, stdDev, mean, min, max etc.
                Dataset<Row> combinedDatasetRatingStats = combinedDatasetUnionParsed.describe("Rating");

                logger.info("***Persisting combinedDatasetUnionParsed to disk***");
                combinedDatasetUnionParsed.persist(StorageLevel.DISK_ONLY()).count();

                logger.info("***Persisting movieTitlesDatasetParsed to disk***");
                movieTitlesDatasetParsed.persist(StorageLevel.DISK_ONLY()).count();

                // // Triggers the persist!
                // combinedDatasetUnionParsed.count();
                // movieTitlesDatasetParsed.count();

                // Build's a very large dataset by combining both the combined and movie tiles
                // dataset after they have been parsed.
                Dataset<Row> combineAndMovieTitlesJoinedDataset = Preprocessing.performLargeJoin(
                                combinedDatasetUnionParsed,
                                movieTitlesDatasetParsed);

                // Build's a large dataset containing information about each movie and it's avg
                // rating.
                Dataset<Row> aggAvgRatingJoinedDataset = Preprocessing.performLargeJoin(
                                aggAvgRatingCombinedDataset,
                                movieTitlesDatasetParsed);

                aggAvgRatingJoinedDataset.persist(StorageLevel.DISK_ONLY()).count();
                // // Triggers the persist!
                // aggAvgRatingJoinedDataset.count();

                Dataset<Row> yearOfReleaseDistribution = aggAvgRatingJoinedDataset
                                .groupBy("YearOfRelease")
                                .count()
                                .orderBy("YearOfRelease");

                writeProcessedDatasetsToDisk(combineAndMovieTitlesJoinedDataset, "combineAndMovieTitlesJoinedDataset");

                writeProcessedDatasetsToDisk(combinedDatasetRatingStats, "combinedDatasetRatingStats");

                writeProcessedDatasetsToDisk(combinedDatasetRatingDistribution, "combinedDatasetRatingDistribution");

                writeProcessedDatasetsToDisk(aggAvgRatingJoinedDataset, "aggAvgRatingJoinedDataset");

                writeProcessedDatasetsToDisk(yearOfReleaseDistribution, "yearOfReleaseDistribution");

                renameOutputfiles();
        }

        static void writeProcessedDatasetsToDisk(Dataset<Row> dataset, String outputFolder) {
                dataset.coalesce(1).write().mode(SaveMode.Overwrite).option("header", "true")
                                .csv(Paths.get(Settings.outputPath).resolve(outputFolder).toString());
        }

        static void renameOutputfiles() {
                logger.info("***Renaming output files***");
                OutputDirConfig.renameOutputfiles("combineAndMovieTitlesJoinedDataset", "csv",
                                "combined_and_movie_titles_joined");

                OutputDirConfig.renameOutputfiles("combinedDatasetRatingStats", "csv",
                                "combined_dataset_rating_stats");

                OutputDirConfig.renameOutputfiles("combinedDatasetRatingDistribution", "csv",
                                "combined_dataset_rating_distribution");

                OutputDirConfig.renameOutputfiles("aggAvgRatingJoinedDataset", "csv",
                                "agg_avg_rating_joinedDataset");

                OutputDirConfig.renameOutputfiles("yearOfReleaseDistribution", "csv",
                                "year_of_release_distribution");
        }
}
