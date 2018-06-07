package com.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * @author AnLuyao
 * @date 2018-06-01 15:54
 */
public class HbaseUtil {
    private static Configuration configuration = HBaseConfiguration.create();

    /**
     * 判断表是否存在
     */
    public static boolean isTableExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;

    }

    /**
     * 初始化命名空间
     *
     * @param namespace
     * @throws IOException
     */
    public static void initNamespace(String namespace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(namespace).addConfiguration("create_ts", String.valueOf(System.currentTimeMillis())).build();
        admin.createNamespace(descriptor);
        close(connection, admin);
    }

    /**
     * 创建表
     */
    public static void createTable(String tableName, int regions,String... columnFamily) throws IOException {
        if (isTableExist(tableName)) {
            System.out.println(tableName + "已存在");
            return;
        }
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : columnFamily) {
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        //添加协处理器
        hTableDescriptor.addCoprocessor("com.coprocessor.CalleeWriteObserver");
        admin.createTable(hTableDescriptor, getSplitKeys(regions));
        close(connection, admin);
    }

    /**
     * 预分区键
     */
    public static byte[][] getSplitKeys(int regions) {
        DecimalFormat df = new DecimalFormat("00");
        byte[][] splitKeys = new byte[regions][];
        for (int i = 0; i < regions; i++) {
            splitKeys[i] = Bytes.toBytes(df.format(i) + "|");
        }
        for (byte[] splitKey : splitKeys) {
            System.out.println(Bytes.toString(splitKey));
        }
        return splitKeys;
    }

    /**
     * 生成rowKey
     * xxx_13651234567_2019-02-21 13:13:13_13891234567_0180
     * regionHash_caller_buildTime_callee_duration
     */
    public static String getRowKey(String regionHash, String caller, String buildTime, String callee,String flag, String duration) {
        return regionHash + "_" + caller + "_" + buildTime + "_" + callee + "_" + flag+"_"+duration;
    }

    /**
     * 生成分区号
     */
    public static String getRegionHash(String caller, String buildTime, int regions) {
        int length = caller.length();
//        获取手机号后4位
        String last4Num = caller.substring(length - 4);
//      获取年月
        String yearMonth = buildTime.replaceAll("-", "").substring(0, 6);
        int regionCode = (Integer.valueOf(last4Num) ^ Integer.valueOf(yearMonth)) % regions;
        DecimalFormat decimalFormat = new DecimalFormat("00");
        return decimalFormat.format(regionCode);

    }

    /**
     * 关闭资源
     */
    private static void close(Connection connection, Admin admin, Table... tables) {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        for (Table table : tables) {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }


}
