package main;

import function.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.HashMap;


/**
 * @Description: spark的WordCount实战
 * @author: willzhao E-mail: zq2599@gmail.com
 * @date: 2019/2/8 17:21
 */
public class WordCount {

    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) {
        //* 设置环境参数
        HashMap<String,String> envSource = new HashMap<String,String> ();
        if(null==args
                || args.length<3
                || StringUtils.isEmpty(args[0])
                || StringUtils.isEmpty(args[1])
                || StringUtils.isEmpty(args[2])) {
            //* localhost
            envSource.put("batch.hostIp","local[*]");
            envSource.put("batch.hostPort","");
            envSource.put("batch.inputFile","input/test1.txt");
            envSource.put("batch.outputFile","output/"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
        }else{

            //* HDFS  server
            String hdfsHost = args[0];
            String hdfsPort = args[1];
            String textFileName = args[2];  //* /input/test.txt
            String inputFile = "hdfs://" + hdfsHost + ":" + hdfsPort + textFileName;
            String outputFile = "hdfs://" + hdfsHost + ":" + hdfsPort +"output/"+ new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            envSource.put("batch.hostIp",hdfsHost);
            envSource.put("batch.hostPort",hdfsPort );
            envSource.put("batch.inputFile",inputFile);
            envSource.put("batch.outputFile",outputFile);
        }
        //count.dataProcess(envSource);//count word from txt file
        countByArrayList.dataProcess(envSource);//count word from arrayList
    }

}
