package com.dao;

import com.utils.ConnectionInstance;
import com.utils.HbaseUtil;
import com.utils.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author AnLuyao
 * @date 2018-06-02 11:16
 * 创建namespace,创建表，put数据
 */
public class HbaseDAO {
    /**
     *  声明相关属性
     */
    private String namespace;
    private String tableName;
    private int regions;
    private String cf;
    private SimpleDateFormat sdf = null;
    private HTable table;
    private String flag;
    /**
     * 缓存put对象的集合
     */
    private List<Put> listPut;

    /**
     * 在创建HbaseDAO时初始化属性及创建NS和table
     * @throws IOException
     */
    public HbaseDAO() throws IOException {
        /**
         * 初始化相关属性（数据来源于配置文件kafka_hbase.properties）
         */
        namespace = PropertiesUtil.properties.getProperty("hbase.namespace");
        tableName = PropertiesUtil.properties.getProperty("hbase.table.name");
        regions = Integer.parseInt(PropertiesUtil.properties.getProperty("hbase.regions"));
        cf = PropertiesUtil.properties.getProperty("hbase.table.cf");
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        listPut = new ArrayList<>();
        flag = "1";

        /**
         * 初始化命名空间及表的创建
         */
        if (!HbaseUtil.isTableExist(tableName)) {
//            HbaseUtil.initNamespace(namespace);
            HbaseUtil.createTable(tableName, regions, cf,"f2");
        }
    }
    /**
     * @param ori 14314302040,19460860743,2019-05-08 23:41:05,0439
     *            rowkey   xxx13651234567_2019-02-21 13:13:13_13891234567_0180
     *            regionHash_caller_buildTime_callee_duration
     *            call1,buildtime,buildtime_ts,call2,duration
     */
    public void put(String ori) throws IOException, ParseException {
        if (listPut.size() == 0) {
            Connection connection = ConnectionInstance.getInstance();
            table = (HTable) connection.getTable(TableName.valueOf(tableName));
            table.setAutoFlushTo(false);
            table.setWriteBufferSize(1024*1024);
        }
        if (ori == null) {
            return;
        }
        String[] split = ori.split(",");
        /**
         * 截取字段封装相关参数
         */
        String caller = split[0];
        String callee = split[1];
        String buildTime = split[2];
        long time = sdf.parse(buildTime).getTime();
        String buildtime_ts = time + "";
        String duration = split[3];
        /**
         * 获取分区号
         */
        String regionHash = HbaseUtil.getRegionHash(caller, buildTime, regions);
        /**
         * 获取rowkey：regionHash_caller_buildTime_callee_duration
         */
        String rowKey = HbaseUtil.getRowKey(regionHash, caller, buildTime, callee,flag, duration);
        /**
         * 为每一条数据创建put对象
         */
        Put put = new Put(Bytes.toBytes(rowKey));
        /**
         * 向put中添加数据（列族：列）（值）
         * call1,buildtime,buildtime_ts,call2,duration
         */
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call1"), Bytes.toBytes(caller));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildtime"), Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildtime_ts"), Bytes.toBytes(buildtime_ts));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call2"), Bytes.toBytes(callee));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("duration"), Bytes.toBytes(duration));
        /**
         * 向put缓存中添加对象
         */
        listPut.add(put);
        /**
         * 当list中数据条数达到20条，则写入HBase
         */
        if (listPut.size() > 20) {
            table.put(listPut);
            table.flushCommits();
            listPut.clear();
            table.close();
        }

    }
}
