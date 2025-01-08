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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparkPipelineTest {
	private static SparkSession spark;

	private StructType getCombinedDatasetSchema() {
		return new StructType(new StructField[] {
				new StructField("MovieID", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("CustomerID", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("Rating", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("Date", DataTypes.DateType, false, Metadata.empty())
		});
	}

	@BeforeClass
	public static void setUp() {
		spark = SparkSession.builder().master("local").appName("SparkPipelineTest").getOrCreate();
	}

	@AfterClass
	public static void tearDown() {
		if (spark != null) {
			spark.stop();
		}
	}

	@Test
	public void testTransformationLogic() {
		List<String> input = Arrays.asList("a", "b", "c", "a", "b", "c", "c");

		Dataset<String> inputData = spark.createDataset(input, Encoders.STRING());

		Dataset<Row> result = inputData.groupBy("value").count();

		List<Row> resultData = result.collectAsList();

		Assert.assertEquals(3, resultData.size());
		Assert.assertTrue(resultData.toString().contains("[c,3]"));
		Assert.assertTrue(resultData.toString().contains("[a,2]"));
		Assert.assertTrue(resultData.toString().contains("[b,2]"));
	}

	@Test
	public void testProcessCombinedDataset() {
		List<String> mockDataset = Arrays.asList("1:", "1488844,3,2005-09-06",
				"1488844,3,2005-09-06", "1488844,3,2005-09-06", "2:", "1567894,4,2005-01-02");

		Dataset<String> inputData = spark.createDataset(mockDataset, Encoders.STRING());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

		Dataset<Row> parsedDataset = inputData.mapPartitions((MapPartitionsFunction<String, Row>) iterator -> {
			List<Row> rows = new ArrayList<>();
			Integer currentMovieID = null;

			while (iterator.hasNext()) {
				System.out.println(iterator.next());
				String line = iterator.next().trim();

				if (line.endsWith(":")) {
					currentMovieID = Integer.parseInt(line.substring(0, line.length() - 1));
				}

				else if (currentMovieID != null) {
					String[] parts = line.split(",");
					Integer customerID = Integer.parseInt(parts[0].trim());
					Integer rating = Integer.parseInt(parts[1].trim());
					Date date = new Date(dateFormat.parse(parts[2].trim()).getTime());
					rows.add(RowFactory.create(currentMovieID, customerID, rating, date));
				}
			}

			return rows.iterator();
		}, Encoders.row(getCombinedDatasetSchema()));
		parsedDataset.count();
		Assert.assertEquals(getCombinedDatasetSchema(), parsedDataset.schema());
	}
}
