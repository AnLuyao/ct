package com.mr;

import com.kv.key.CommDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author AnLuyao
 * @date 2018-06-05 15:41
 */
public class CountDurationDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置&job对象
        Configuration configuration = HBaseConfiguration.create();
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
