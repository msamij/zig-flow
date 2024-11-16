package com.msamiaj.zigflow;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

        // Dataset<Row> combinedData1 = spark
        // .read()
        // .format("text")
        // .load(Settings.projectRoot + "/datasets/combined_data_1.txt");

        // Dataset<Row> combinedData2 = spark
        // .read()
        // .format("text")
        // .load(Settings.projectRoot + "/datasets/combined_data_2.txt");

        // Dataset<Row> combinedData3 = spark
        // .read()
        // .format("text")
        // .load(Settings.projectRoot + "/datasets/combined_data_3.txt");

        // Dataset<Row> combinedData4 = spark
        // .read()
        // .format("text")
        // .load(Settings.projectRoot + "/datasets/combined_data_4.txt");

        // combinedData1.show(5);

        // combinedData2.show(5);

        // combinedData3.show(5);

        // combinedData4.show(5);

        // Dataset<Row> union = combinedData1
        // .unionByName(combinedData2, false)
        // .unionByName(combinedData3, false)
        // .unionByName(combinedData4, false);

        // union.show(10);

        // StructType schema = new StructType(new StructField[] {
        // new StructField("MovieID", DataTypes.IntegerType, false, Metadata.empty()),
        // new StructField("CustomerID", DataTypes.IntegerType, false,
        // Metadata.empty()),
        // new StructField("Rating", DataTypes.IntegerType, false, Metadata.empty()),
        // new StructField("Date", DataTypes.DateType, false, Metadata.empty())
        // });

        // SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        // Dataset<Row> parsedDataset = union.mapPartitions((MapPartitionsFunction<Row,
        // Row>) iterator -> {
        // List<Row> rows = new ArrayList<>();
        // Integer currentMovieID = null;

        // while (iterator.hasNext()) {
        // String line = iterator.next().getString(0).trim();

        // // MovieID (example:-> 1:)
        // if (line.endsWith(":")) {
        // currentMovieID = Integer.parseInt(line.substring(0, line.length() - 1));
        // }

        // // line contains customerID, rating, date (example:-> "1488844,3,2005-09-06")
        // else if (currentMovieID != null) {
        // String[] parts = line.split(",");
        // Integer customerID = Integer.parseInt(parts[0].trim());
        // Integer rating = Integer.parseInt(parts[1].trim());
        // Date date = new Date(dateFormat.parse(parts[2].trim()).getTime());

        // rows.add(RowFactory.create(currentMovieID, customerID, rating, date));
        // }
        // }

        // return rows.iterator();
        // }, Encoders.row(schema));

        // parsedDataset = parsedDataset.withColumn("Date",
        // date_format(parsedDataset.col("Date"), "dd-MM-yyyy"));

        // parsedDataset.show(10);
        // parsedDataset.printSchema();

        // StructType movieTitleSchema = new StructType(new StructField[] {
        // new StructField("MovieID", DataTypes.IntegerType, false, Metadata.empty()),
        // new StructField("YearOfRelease", DataTypes.StringType, false,
        // Metadata.empty()),
        // new StructField("Title", DataTypes.StringType, false, Metadata.empty())
        // });

        // Dataset<Row> movieTitles = spark.read()
        // .option("inferSchema", "true")
        // .option("header", "false")
        // .option("delimiter", "\t")
        // .option("multiline", "true")
        // .csv(Settings.projectRoot + "/datasets/movie_titles.csv");

        // Dataset<Row> fixedDataset =
        // movieTitles.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
        // List<Row> rows = new ArrayList<>();

        // Row row = null;
        // String[] data = null;
        // int movieID = -1;
        // String yearOfRelease = null;
        // String title = null;

        // while (iterator.hasNext()) {
        // row = iterator.next();
        // data = row.getString(0).split(",");
        // movieID = Integer.parseInt(data[0].trim());
        // yearOfRelease = data[1].trim();
        // title = String.join(",", Arrays.copyOfRange(data, 2, data.length)).trim();
        // rows.add(RowFactory.create(movieID, yearOfRelease, title));
        // }
        // return rows.iterator();
        // }, Encoders.row(movieTitleSchema));

        // fixedDataset.show(80, false);
        // fixedDataset.printSchema();

        // Dataset<Row> combinedDataset = parsedDataset.join(fixedDataset, "MovieID");
        // combinedDataset.show(1500, false);
        // combinedDataset.printSchema();

        // combinedDataset.filter("Title IS NULL OR YearOfRelease IS NULL").show();
    }
}
