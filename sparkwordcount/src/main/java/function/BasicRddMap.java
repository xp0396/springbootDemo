package function;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

//
// This very basic example creates an RDD from a list of integers. It Explores
// how to transform the RDD element by element, convert it back to an array, and
// examine its partitioning. Also it begins to explore the fact that RDDs are in
// practice order preserving but when you operate on them directly their
// distributed nature prevents them from behaving like they are ordered.
//
public class BasicRddMap {
    public static void main(String[] args) {
        //
        // The "modern" way to initialize Spark is to create a SparkSession
        // although they really come from the world of Spark SQL, and Dataset
        // and DataFrame.
        //
        SparkSession spark = SparkSession
                .builder()
                .appName("RDD-Basic")
                .master("local[4]")  //4 代表用4个线程处理
                .getOrCreate();

        //
        // Operating on a raw RDD actually requires access to the more low
        // level SparkContext -- get the special Java version for convenience
        //
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // put some data in an RDD
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        //
        // Since this SparkContext is actually a JavaSparkContext, the methods
        // return a JavaRDD, which is more convenient as well.
        //  根据系统环境来进行切分多个slice，每一个slice启动一个Task来进行处理
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers, 4);
        System.out.println("*** Print each element of the original RDD");
        System.out.println("*** (they won't necessarily be in any order)");
        // Since printing is delegated to the RDD it happens in parallel.
        // For versions of Java without lambda, Spark provides some utility
        // interfaces like VoidFunction.

        // NOTE: in the above it may be tempting to replace the above
        // lambda expression with a method reference -- System.out::println --
        // but alas this results in
        //     java.io.NotSerializableException: java.io.PrintStream

        // Transform the RDD element by element -- this time use Function
        // instead of a Lambda. Notice how the RDD changes from
        // JavaRDD<Integer> to JavaRDD<double>.
        JavaRDD<Double> transformedRDD =
                numbersRDD.map(n -> new Double(n) / 10);

        // let's see the elements
        System.out.println("*** Print each element of the transformed RDD");
        System.out.println("*** (they may not even be in the same order)");
        transformedRDD.foreach(i -> System.out.println(i));

        spark.stop();
    }
}