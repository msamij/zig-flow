package com.msamiaj.zigflow.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public final class Schema {
	private Schema() {
		throw new UnsupportedOperationException("Schema class must not be instantiated!");
	}

	public static StructType getCombinedDatasetSchema() {
		return new StructType(new StructField[] {
				new StructField("MovieID", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("CustomerID", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("Rating", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("Date", DataTypes.DateType, false, Metadata.empty())
		});

	}

	public static StructType getMovieTitleDatasetSchema() {
		return new StructType(new StructField[] {
				new StructField("MovieID", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("YearOfRelease", DataTypes.StringType, false, Metadata.empty()),
				new StructField("Title", DataTypes.StringType, false, Metadata.empty())
		});
	}
}
