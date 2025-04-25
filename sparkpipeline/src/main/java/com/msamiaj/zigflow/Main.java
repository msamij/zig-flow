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

@SuppressWarnings("unused")
public class Main {
        private static Ingestion ingestion;
        private static final String APP_NAME = "zigflow";
        private static final String MASTER = "local";
        private static final String DRIVER_MEMORY = "6g";
        private static final String EXECUTOR_MEMORY = "4g";
        private static final Logger logger = LoggerFactory.getLogger(Main.class);

        public static void main(String[] args) {
                SparkSession spark = getSparkSession();
                spark.sparkContext().setCheckpointDir(Settings.checkpointPath);

                logger.info("Spark driver memory config:" + spark.sparkContext().getConf().get("spark.driver.memory"));

                ingestion = new Ingestion(spark);

                // Dataframes that have not been parsed or transformed yet!
                Dataset<Row> combinedDatasetUnionRaw = ingestion.ingestCombinedDataFiles();
                Dataset<Row> movieTitlesDatasetRaw = ingestion.ingestMovieTitlesFile();

                // Spark now stores the DAG containing transformations for Dataframes!
                Dataset<Row> combinedDatasetUnionParsed = Preprocessing.processCombinedDataset(combinedDatasetUnionRaw);
                Dataset<Row> movieTitlesDatasetParsed = Preprocessing.processMovieTitlesDataset(movieTitlesDatasetRaw);

                // Triggers the cache!
                logger.info("***Persisting combinedDatasetUnionParsed to disk***");
                cacheDataset(combinedDatasetUnionParsed);

                // Get's average rating for each movie and rating count (i.e no of rating each
                // movie had received).
                Dataset<Row> aggAvgRatingCombinedDataset = combinedDatasetUnionParsed.groupBy("MovieID")
                                .agg(avg("Rating").alias("AvgRating"), count("Rating").alias("RatingCount"));

                // Get's the rating distribution, i.e how much count each rating value have.
                Dataset<Row> combinedDatasetRatingDistribution = combinedDatasetUnionParsed.groupBy("Rating").count();

                // Get the descriptive stats for Rating col, stdDev, mean, min, max etc.
                Dataset<Row> combinedDatasetRatingStats = combinedDatasetUnionParsed.describe("Rating");

                // Triggers the cache!
                logger.info("***Persisting movieTitlesDatasetParsed to disk***");
                cacheDataset(movieTitlesDatasetParsed);

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

                // Triggers the cache!
                logger.info("***Persisting aggAvgRatingJoinedDataset to disk***");
                cacheDataset(aggAvgRatingJoinedDataset);

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

        private static SparkSession getSparkSession() {
                return SparkSession.builder().config(new SparkConf().setAppName(APP_NAME)).getOrCreate();
        }

        private static void cacheDataset(Dataset<Row> dataset) {
                dataset.persist(StorageLevel.MEMORY_ONLY()).count();
        }

        private static void writeProcessedDatasetsToDisk(Dataset<Row> dataset, String outputFolder) {
                dataset.coalesce(1).write().mode(SaveMode.Overwrite).option("header", "true")
                                .csv(Paths.get(Settings.outputPath).resolve(outputFolder).toString());
        }

        private static void renameOutputfiles() {
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
