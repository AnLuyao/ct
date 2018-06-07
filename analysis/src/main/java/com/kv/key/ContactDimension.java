package com.kv.key;

import com.kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author AnLuyao
 * @date 2018-06-04 15:31
 */
public class ContactDimension extends BaseDimension {

    private String name;
    private String phoneNum;

    public ContactDimension() {
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name + "\t" + phoneNum;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    @Override
    public int compareTo(BaseDimension o) {
        ContactDimension other = (ContactDimension) o;
        return phoneNum.compareTo(other.phoneNum);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(phoneNum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.phoneNum = in.readUTF();
    }
}
