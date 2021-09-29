package RddBasicFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.api.java.function.Function;
import java.util.List;

public class BasicRddfilter {
    private  static final Logger logger = LoggerFactory.getLogger(BasicRddfilter.class);
    public static void main(String[] args) throws Exception {
        //创建RDD
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("filter").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        JavaRDD<String>  inputFile = sc.textFile("input/filter.txt");
        System.out.println("***inputFile 加载后的RDD 元素***");
        inputFile.foreach(i -> System.out.println(i));
        //1.Transformation：filter筛选转换
        //2.Actions：collect执行
        //case pass函数
        List<String> filterRDD =  inputFile.filter(new filter_0()).collect();
        System.out.println("***filterRDD筛选结果输出***");
        for (String a:filterRDD) {
            System.out.println(a);
        }
        //lamberd 表达式，效果等同filter_0()
        List<String> filterRDD1 =  inputFile.filter((s) -> StringUtils.contains(s,"filter") ).collect();
        System.out.println("***filterRDD1筛选结果输出***");
        for (String b:filterRDD1) {
            System.out.println(b);
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


