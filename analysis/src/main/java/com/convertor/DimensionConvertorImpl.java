package com.convertor;

import com.kv.base.BaseDimension;
import com.kv.key.ContactDimension;
import com.kv.key.DateDimension;
import com.util.JDBCInstance;
import com.util.LRUCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author AnLuyao
 * @date 2018-06-05 10:29
 */
public class DimensionConvertorImpl implements IConvertor {

    private LRUCache lruCache = new LRUCache(3000);

    @Override
    public int getDimensionID(BaseDimension baseDimensionn) {

        //1.查询缓存是否有值
        String cacheKey = getCacheKey(baseDimensionn);
        if (lruCache.containsKey(cacheKey)) {
            return lruCache.get(cacheKey);//缓存中有值
        }

        //2.获取mysql中是否有值,如果没有，插入数据
        String[] sqls = getSqls(baseDimensionn);

        Connection connection = null;
        try {
            connection = JDBCInstance.getInstance();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //3.执行sql（查询，插入，查询）
        int id = -1;
        try {
            id = execSql(sqls, connection, baseDimensionn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (id == -1) throw new RuntimeException("未匹配到相应维度！");

        //5.将数据放入缓存
        lruCache.put(cacheKey, id);

        //4.返回id
        return id;
    }

    private int execSql(String[] sqls, Connection connection, BaseDimension baseDimensionn) throws SQLException {
        int id = -1;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sqls[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //第一次查询
        setArguments(preparedStatement, baseDimensionn);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }
        //查询不到，插入数据
        preparedStatement = connection.prepareStatement(sqls[1]);
        setArguments(preparedStatement, baseDimensionn);
        preparedStatement.executeUpdate();

        //第二次查询
        preparedStatement = connection.prepareStatement(sqls[0]);
        setArguments(preparedStatement, baseDimensionn);
        resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }
        return id;
    }

    private void setArguments(PreparedStatement preparedStatement, BaseDimension baseDimensionn) throws SQLException {
        //给预编译sql赋值
        int i = 0;
        if (baseDimensionn instanceof ContactDimension) {
            ContactDimension contactDimension = (ContactDimension) baseDimensionn;
            preparedStatement.setString(++i, contactDimension.getPhoneNum());
            preparedStatement.setString(++i, contactDimension.getName());
        } else {
            DateDimension dateDimension = (DateDimension) baseDimensionn;
            preparedStatement.setInt(++i, Integer.valueOf(dateDimension.getYear()));
            preparedStatement.setInt(++i, Integer.valueOf(dateDimension.getMonth()));
            preparedStatement.setInt(++i, Integer.valueOf(dateDimension.getDay()));
        }
    }

    //根据不同的维度封装sql
    private String[] getSqls(BaseDimension baseDimensionn) {
        String[] sqls = new String[2];
        if (baseDimensionn instanceof ContactDimension) {
//            ContactDimension contactDimension = (ContactDimension) baseDimensionn;
//            contactDimension.getPhoneNum();
            sqls[0] = "SELECT `id` FROM `tb_contacts` WHERE `telephone` = ? AND `name` = ?";
            sqls[1] = "INSERT INTO tb_contacts VALUES(NULL,?,?);";
        } else if (baseDimensionn instanceof DateDimension) {
            sqls[0] = "SELECT `id` FROM `tb_dimension_date` WHERE `year` = ? AND month = ? AND day = ?";
            sqls[1] = "INSERT INTO `tb_dimension_date` VALUES(NULL,?,?,?);";
        }
        return sqls;
    }

    //获取tb_call表主键
    private String getCacheKey(BaseDimension baseDimensionn) {
        StringBuffer sb = new StringBuffer();
        if (baseDimensionn instanceof ContactDimension) {
            ContactDimension contactDimension = (ContactDimension) baseDimensionn;
            sb.append(contactDimension.getPhoneNum());
        } else if (baseDimensionn instanceof DateDimension) {
            DateDimension dateDimension = (DateDimension) baseDimensionn;
            sb.append(dateDimension.getYear()).append(dateDimension.getMonth()).append(dateDimension.getDay());
        }
        return sb.toString();
    }

}

