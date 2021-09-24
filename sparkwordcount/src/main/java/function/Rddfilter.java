package function;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.api.java.function.Function;
import java.util.List;

public class Rddfilter {
    private  static final Logger logger = LoggerFactory.getLogger(Rddfilter.class);
    public static void main(String[] args) throws Exception {
        //创建RDD
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("filter").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        JavaRDD<String>  inputFile = sc.textFile("input/filter.txt");
        //1.Transformation：filter筛选转换
        //2.Actions：collect执行
        List<String> filterRDD =  inputFile.filter(new filter_0()).collect();
        inputFile.foreach(i -> System.out.println(i));
        //结果打印出来
        for (String a:filterRDD) {
            logger.info(a);
        }
         //关闭
        sc.close();
    }
    //包含字符"filter" 的元素
    public static class filter_0  implements Function<String,Boolean>{
        @Override
        public Boolean call(String S1) throws Exception{
            if (StringUtils.contains(S1,"filter")){
                return  true;
            }else{
                return  false;
            }
        }
    }

}


