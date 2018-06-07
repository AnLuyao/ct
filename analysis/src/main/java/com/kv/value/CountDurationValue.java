package com.kv.value;

import com.kv.base.BaseValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author AnLuyao
 * @date 2018-06-04 16:47
 */
public class CountDurationValue extends BaseValue {

    private String countSum;
    private String durationSum;

    public CountDurationValue() {
    }

    public String getCountSum() {
        return countSum;
    }

    public void setCountSum(String countSum) {
        this.countSum = countSum;
    }

    public String getDurationSum() {
        return durationSum;
    }

    public void setDurationSum(String durationSum) {
        this.durationSum = durationSum;
    }

    @Override
    public String toString() {
        return countSum + "\t" + durationSum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(countSum);
        out.writeUTF(durationSum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.countSum = in.readUTF();
        this.durationSum = in.readUTF();
    }
}
