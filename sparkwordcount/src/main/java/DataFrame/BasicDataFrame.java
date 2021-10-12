package DataFrame;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BasicDataFrame {
    public static void main(String[] args) {
        //
        // The "modern" way to initialize Spark is to create a SparkSession
        // although they really come from the world of Spark SQL, and Dataset
        // and DataFrame.
        //
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL Example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("input//people.json");
        df.show();

    }
}
