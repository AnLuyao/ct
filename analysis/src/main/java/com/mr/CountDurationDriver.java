package com.mr;

import com.kv.key.CommDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;

import java.io.IOException;

/**
 * @author AnLuyao
 * @date 2018-06-05 15:41
 */
public class CountDurationDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "slash");

        Configuration configuration = HBaseConfiguration.create();
        // 是否跨平台提交任务
        configuration.set(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, "true");
        // 究竟运行在本地还是在集群
        configuration.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
        // jar包
        configuration.set(MRJobConfig.JAR, "D:\\devlope\\workspace\\ct\\analysis\\target\\analysis-1.0-SNAPSHOT-jar-with-dependencies.jar");

        Job job = Job.getInstance(configuration);

        //2.设置jar
        job.setJarByClass(CountDurationDriver.class);

        Scan scan = new Scan();

        //3.设置Mapper
        TableMapReduceUtil.initTableMapperJob("ct:calllog",
                scan,
                CountDurationMapper.class,
                CommDimension.class,
                Text.class,
                job);

        //4.设置Reducer
        job.setReducerClass(CountDurationReducer.class);

        //5.设置输出类型&自定义的OutPutFormat
        job.setOutputFormatClass(MySQLOutPutFormat.class);

//        job.setOutputKeyClass();
//        job.setOutputValueClass();

        //6.提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
