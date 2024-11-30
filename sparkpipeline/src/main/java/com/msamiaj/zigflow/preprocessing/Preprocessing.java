package com.msamiaj.zigflow.preprocessing;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import com.msamiaj.zigflow.schema.Schema;

public class Preprocessing {
	public static Dataset<Row> processCombinedDataset(Dataset<Row> unionDataset) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

		Dataset<Row> parsedDataset = unionDataset.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
			List<Row> rows = new ArrayList<>();
			Integer currentMovieID = null;

			while (iterator.hasNext()) {
				String line = iterator.next().getString(0).trim();

				// MovieID (example:-> 1:)
				if (line.endsWith(":")) {
					currentMovieID = Integer.parseInt(line.substring(0, line.length() - 1));
				}

				// line contains customerID, rating, date (example:-> "1488844,3,2005-09-06")
				else if (currentMovieID != null) {
					String[] parts = line.split(",");
					Integer customerID = Integer.parseInt(parts[0].trim());
					Integer rating = Integer.parseInt(parts[1].trim());
					Date date = new Date(dateFormat.parse(parts[2].trim()).getTime());

					rows.add(RowFactory.create(currentMovieID, customerID, rating, date));
				}
			}

			return rows.iterator();
		}, Encoders.row(Schema.getCombinedDatasetSchema()));

		parsedDataset = parsedDataset.withColumn("Date", date_format(parsedDataset.col("Date"), "dd-MM-yyyy"));
		return parsedDataset;
	}

	public static Dataset<Row> processMovieTitlesDataset(Dataset<Row> movieTitlesDataset) {
		Dataset<Row> parsedDataset = movieTitlesDataset
				.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
					List<Row> rows = new ArrayList<>();

					Row row = null;
					String[] data = null;
					int movieID = -1;
					String yearOfRelease = null;
					String title = null;

					while (iterator.hasNext()) {
						row = iterator.next();
						data = row.getString(0).split(",");
						movieID = Integer.parseInt(data[0].trim());
						yearOfRelease = data[1].trim();
						title = String.join(",", Arrays.copyOfRange(data, 2, data.length))
								.trim();
						rows.add(RowFactory.create(movieID, yearOfRelease, title));
					}
					return rows.iterator();
				}, Encoders.row(Schema.getMovieTitleDatasetSchema()));

		parsedDataset = sanityCheckMovieTitlesDataset(parsedDataset);
		return parsedDataset;
	}

	@SuppressWarnings("unused")
	private static Dataset<Row> sanityCheckCombinedDataset(Dataset<Row> dataset) {
		return dataset.filter(col("MovieID").isNotNull()
				.and(col("CustomerID").isNotNull())
				.and(col("Rating").isNotNull())
				.and(col("Date").isNotNull()))

				.filter(col("MovieID").notEqual("NULL")
						.and(col("CustomerID").notEqual("NULL"))
						.and(col("Rating").notEqual("NULL"))
						.and(col("Date").notEqual("NULL")));
	}

	private static Dataset<Row> sanityCheckMovieTitlesDataset(Dataset<Row> dataset) {
		return dataset.filter((col("YearOfRelease").notEqual("NULL")));
	}

	/**
	 * Perform inner join and build a large dataset based on MovieID column.
	 * 
	 * @param aggregatedcombinedDataset Dataframe of combined dataset that
	 *                                  have been
	 *                                  parsed.
	 * @param movieTitlesParsedDataset  Dataframe of movie titles dataset that
	 *                                  have
	 *                                  been parsed.
	 * @return a dataframe of combined dataset that have been joined with movie
	 *         titles dataset based on "MovieID" column.
	 */
	public static Dataset<Row> performLargeJoin(
			Dataset<Row> aggregatedcombinedDataset,
			Dataset<Row> movieTitlesParsedDataset) {
		return aggregatedcombinedDataset.join(movieTitlesParsedDataset, "MovieID");
	}
}
