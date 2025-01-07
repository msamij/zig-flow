import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparkPipelineTest {
	private static SparkSession spark;

	@BeforeClass
	public static void setUp() {
		spark = SparkSession.builder()
				.master("local[*]")
				.appName("SparkPipelineTest")
				.getOrCreate();
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
}
