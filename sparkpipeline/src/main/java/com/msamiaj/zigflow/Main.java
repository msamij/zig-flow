package com.msamiaj.zigflow;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.count;

import java.nio.file.Paths;

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

                logger.info("***Adding persist to combinedDatasetUnionParsed***");
                combinedDatasetUnionParsed.persist(StorageLevel.DISK_ONLY());

                logger.info("***Adding persist to movieTitlesDatasetParsed***");
                movieTitlesDatasetParsed.persist(StorageLevel.DISK_ONLY());

                // Triggers the persist!
                combinedDatasetUnionParsed.count();
                movieTitlesDatasetParsed.count();

                // Get's average rating for each movie and rating count (i.e no of rating each
                // movie had received).
                Dataset<Row> aggAvgRatingCombinedDataset = combinedDatasetUnionParsed.groupBy("MovieID")
                                .agg(avg("Rating").alias("AvgRating"), count("Rating").alias("RatingCount"));

                aggAvgRatingCombinedDataset.show(20, false);
                // Get the descriptive stats for Rating col, stdDev, mean, min, max etc.
                Dataset<Row> combinedDatasetRatingStats = combinedDatasetUnionParsed.describe("Rating");

                // Get's the rating distribution, i.e how much count each rating value have.
                Dataset<Row> combinedDatasetRatingDistribution = combinedDatasetUnionParsed.groupBy("Rating").count();

                // Build's a very large dataset by combining both the combined and movie tiles
                // dataset after they have been parsed.
                Dataset<Row> combineAndMovieTitlesJoinedDataset = Preprocessing.performLargeJoin(
                                combinedDatasetUnionParsed,
                                movieTitlesDatasetParsed);

                Dataset<Row> aggAvgRatingJoinedDataset = Preprocessing.performLargeJoin(
                                aggAvgRatingCombinedDataset,
                                movieTitlesDatasetParsed);

                aggAvgRatingJoinedDataset.persist(StorageLevel.DISK_ONLY());
                aggAvgRatingJoinedDataset.show(20, false);

                Dataset<Row> yearOfReleaseDistribution = aggAvgRatingJoinedDataset
                                .groupBy("YearOfRelease")
                                .count()
                                .orderBy("YearOfRelease");

                logger.info("***Writing combineAndMovieTitlesJoinedDataset***");
                combineAndMovieTitlesJoinedDataset
                                .coalesce(1)
                                .write()
                                .option("header", "true")
                                .csv(Paths.get(Settings.outputPath).resolve("combineAndMovieTitlesJoinedDataset")
                                                .toString());

                logger.info("***Writing combinedDatasetRatingStats for visualization***");
                combinedDatasetRatingStats
                                .coalesce(1)
                                .write()
                                .option("header", "true")
                                .csv(Paths.get(Settings.outputPath).resolve("combinedDatasetRatingStats").toString());

                logger.info("***Writing combinedDatasetRatingDistribution for visualization***");
                combinedDatasetRatingDistribution
                                .coalesce(1)
                                .write()
                                .option("header", "true")
                                .csv(Paths.get(Settings.outputPath).resolve("combinedDatasetRatingDistribution")
                                                .toString());

                logger.info("***Writing aggAvgRatingJoinedDataset for visualization***");
                aggAvgRatingJoinedDataset
                                .coalesce(1)
                                .write()
                                .option("header", "true")
                                .csv(Paths.get(Settings.outputPath).resolve("aggAvgRatingJoinedDataset").toString());

                logger.info("***Writing yearOfReleaseDistribution for visualization***");
                yearOfReleaseDistribution
                                .coalesce(1)
                                .write()
                                .option("header", "true")
                                .csv(Paths.get(Settings.outputPath).resolve("yearOfReleaseDistribution").toString());

                logger.info("***Renaming output files***");

                OutputDirConfig.renameOutputfiles("combineAndMovieTitlesJoinedDataset", "csv",
                                "combined_and_movie_titles");

                OutputDirConfig.renameOutputfiles("combinedDatasetRatingStats", "csv",
                                "combined_dataset_rating_stats");

                OutputDirConfig.renameOutputfiles("combinedDatasetRatingDistribution", "csv",
                                "combined_dataset_rating_distribution");

                OutputDirConfig.renameOutputfiles("aggAvgRatingJoinedDataset", "csv",
                                "agg_avg_rating_joined");

                OutputDirConfig.renameOutputfiles("yearOfReleaseDistribution", "csv",
                                "year_of_release_distribution");
        }
}
