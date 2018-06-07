package com.mr;

import com.kv.key.CommDimension;
import com.kv.value.CountDurationValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author AnLuyao
 * @date 2018-06-04 16:43
 */
public class CountDurationReducer extends Reducer<CommDimension, Text, CommDimension, CountDurationValue> {

    private CountDurationValue v = new CountDurationValue();

    @Override
    protected void reduce(CommDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int countSum = 0;//通话总次数
        int durationSum = 0;//通话总时长

        //循环累加
        for (Text value : values) {
            countSum++;
            durationSum += Integer.valueOf(value.toString());
        }

        //设置value的值
        v.setCountSum(countSum + "");
        v.setDurationSum(durationSum + "");

        //写出去
        context.write(key, v);
    }
}
