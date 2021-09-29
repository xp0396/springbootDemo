package function;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class BasicRddflatMap {
    private  static final Logger logger = LoggerFactory.getLogger(BasicRddflatMap.class);
    public static void main(String[] args) throws Exception {
        //创建RDD
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("filter").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        JavaRDD<String>  lineFile = sc.textFile("input/filter.txt");
        System.out.println("***inputFile 加载后的RDD 元素***");
        lineFile.foreach(i -> System.out.println(i));

        JavaRDD<String> wordRDD =  lineFile.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        System.out.println("***wordRDD筛选结果输出***");
        wordRDD.foreach(a->System.out.println("元素："+a));
         //关闭
        sc.close();
    }



}


