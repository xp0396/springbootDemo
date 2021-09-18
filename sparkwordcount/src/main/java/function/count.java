package function;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class count {
    private static final Logger logger = LoggerFactory.getLogger(count.class);
    public static void   dataProcess(HashMap<String,String> envSource) {
        // 主机IP
        String hdfsHost = envSource.get("batch.hostIp");
        // 主机端口
        String hdfsPort = envSource.get("batch.hostPort");
        //文本文件的hdfs路径
        String inputPath = envSource.get("batch.inputFile");
        //输出结果文件的路径
        String outputPath  =  envSource.get("batch.outputFile");
        //具体位置
        SparkConf sparkConf = new SparkConf();

        if (StringUtils.equals(hdfsHost,"local[*]")) {
            sparkConf.setMaster(hdfsHost).setAppName("Spark WordCount Application (java)");
        }else{
            //hdfs
            sparkConf.setAppName("Spark WordCount Application (java)");
        }


        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        logger.info("input path : {}", inputPath);
        logger.info("output path : {}", outputPath);

        logger.info("import text");
        //导入文件
        JavaRDD<String> textFile = javaSparkContext.textFile(inputPath);

        logger.info("do map operation");
        JavaPairRDD<String, Integer> counts = textFile
                //每一行都分割成单词，返回后组成一个大集合
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                //key是单词，value是1
                .mapToPair(word -> new Tuple2<>(word, 1))
                //基于key进行reduce，逻辑是将value累加
                .reduceByKey((a, b) -> a + b);

        logger.info("do convert");
        //先将key和value倒过来，再按照key排序
        JavaPairRDD<Integer, String> sorts = counts
                //key和value颠倒，生成新的map
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2(), tuple2._1()))
                //按照key倒排序
                .sortByKey(false);

        logger.info("take top 10");
        //取前10个
        List<Tuple2<Integer, String>> top10 = sorts.take(10);

        StringBuilder sbud = new StringBuilder("top 10 word :\n");

        //打印出来
        for(Tuple2<Integer, String> tuple2 : top10){
            sbud.append(tuple2._2())
                    .append("\t")
                    .append(tuple2._1())
                    .append("\n");
        }

        logger.info(sbud.toString());

        logger.info("merge and save as file");
        //分区合并成一个，再导出为一个txt保存在hdfs
        javaSparkContext.parallelize(top10).coalesce(1).saveAsTextFile(outputPath);

        logger.info("close context");
        //关闭context
        javaSparkContext.close();
    }

}
