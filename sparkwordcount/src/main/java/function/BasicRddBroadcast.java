package function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class BasicRddBroadcast {
    private  static final Logger logger = LoggerFactory.getLogger(BasicRddBroadcast.class);
    public static void main(String[] args) throws Exception {
        //创建RDD
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("filter").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});
        System.out.println("broadcastVar："+broadcastVar.value());
         //关闭
        sc.close();
    }



}


