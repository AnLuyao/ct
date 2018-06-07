package com.coprocessor;

import com.utils.ConnectionInstance;
import com.utils.HbaseUtil;
import com.utils.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author AnLuyao
 * @date 2018-06-02 18:54
 */
public class CalleeWriteObserver extends BaseRegionObserver {
    private int regions = Integer.valueOf(PropertiesUtil.properties.getProperty("hbase.regions"));
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        /**
         * 00_15961260091_2019-03-05 10:04:05_13157770954_1_0673
         0x_13157770954_2019-03-05 10:04:05_15961260091_0_0673
         */
        //获取之前操作的表
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        String curTableName = PropertiesUtil.properties.getProperty("hbase.table.name");
        if (!tableName.equals(curTableName)) {
            return;
        }

        //获取之前数据的rowkey
        String row = Bytes.toString(put.getRow());

        //00_15961260091_2019-03-05 10:04:05_13157770954_1_0673
        String[] split = row.split("_");
        String flag = split[4];
        if ("0".equals(flag)) return;

        String caller = split[1];
        String buildTime = split[2];
        String buildtime_ts=null;
        try {
            buildtime_ts = sdf.parse(buildTime).getTime() + "";
        } catch (ParseException e1) {
            e1.printStackTrace();
        }

        String callee = split[3];
        String duration = split[5];

        //获取分区号
        String regionHash = HbaseUtil.getRegionHash(callee, buildTime, regions);
        //获取rowkey
        String rowKey = HbaseUtil.getRowKey(regionHash, callee, buildTime, caller, "0", duration);

        Put newPut = new Put(Bytes.toBytes(rowKey));

        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildtime"), Bytes.toBytes(buildTime));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildtime_ts"), Bytes.toBytes(buildtime_ts));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes("0"));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //获取连接
        Connection connection = ConnectionInstance.getInstance();
        Table table = connection.getTable(TableName.valueOf(tableName));

        table.put(newPut);
        table.close();
    }
}
