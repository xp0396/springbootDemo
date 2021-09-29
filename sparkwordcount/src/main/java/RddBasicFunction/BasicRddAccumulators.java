package RddBasicFunction;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;


import java.util.Arrays;
import java.util.List;

// 这个案例是将数组的元素累加到全局变量中
//   如果在本地执行，可能可以正常累加
//   如果放在分布式的集群中，应master 会拆分出多个进程，在executor中执行，但是master 并不能读取到相关的副本变量值
//   解决办法：Use an Accumulator instead if some global aggregation is needed.

//   另外一种案例:如打印using rdd.foreach(println) or rdd.map(println)
//   这种情况下，在分布式集群中运行，都是在节点的 executor 上跑并输出，而不是在驱动程序上输出
//   解决办法1： 使用 collect（）将RDD带到驱动程序节点 rdd.collect().foreach(println)
//   解决办法2： 使用 take（） 将RDD带到驱动程序节点 RDD.take（100）.foreach（println）

public class BasicRddAccumulators {
    private static Integer counter =0;

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("RDD-Basic")
                .master("local[4]")  //4 代表用4个线程处理
                .getOrCreate();

        // Operating on a raw RDD actually requires access to the more low
        // level SparkContext -- get the special Java version for convenience
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        // put some data in an RDD
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers, 4);
        // 错误用法：counter 是全局变量
        // 正确用法：LongAccumulator
        LongAccumulator accum = sc.sc().longAccumulator();
        numbersRDD.foreach(x-> {counter +=x;accum.add(x);});
        System.out.println("counter:"+counter);
        System.out.println("accum.value:"+accum.value());
        spark.stop();
    }






}