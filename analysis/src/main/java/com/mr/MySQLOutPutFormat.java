package com.mr;


import com.convertor.DimensionConvertorImpl;
import com.kv.base.BaseDimension;
import com.kv.key.CommDimension;
import com.kv.value.CountDurationValue;
import com.util.JDBCInstance;
import com.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author AnLuyao
 * @date 2018-06-05 9:28
 */
public class MySQLOutPutFormat extends OutputFormat<BaseDimension, CountDurationValue> {

    private FileOutputCommitter committer = null;

    @Override
    public RecordWriter<BaseDimension, CountDurationValue> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        Connection connection = null;
        try {
            connection = JDBCInstance.getInstance();
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new MysqlRecordWriter(connection);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    private static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    static class MysqlRecordWriter extends RecordWriter<BaseDimension, CountDurationValue> {
        //声明相关属性
        private Connection connection = null;//JDBC 连接
        private PreparedStatement preparedStatement = null; //预编译sql
        private int batchBound = 500;//缓存sql条数边界
        private int batchSize = 0;//客户端已经缓存的条数

        public MysqlRecordWriter(Connection connection) {
            this.connection = connection;//初始化JDBC连接
        }

        @Override
        public void write(BaseDimension key, CountDurationValue value) throws IOException, InterruptedException {

            CommDimension commDimension = (CommDimension) key;
            //插入数据到mysql
            String sql = "INSERT INTO ct.tb_call VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE `call_sum`=?, `call_duration_sum`=?;";

            //维度转换
            DimensionConvertorImpl convertor = new DimensionConvertorImpl();

            //获取联系人维度的id
            int dateId = convertor.getDimensionID(commDimension.getDateDimension());
            //获取时间维度id
            int contactId = convertor.getDimensionID(commDimension.getContactDimension());

            //拼接tb_call表的主键
            String date_contact = dateId + "_" + contactId;

            //获取通话总次数
            int countSum = Integer.valueOf(value.getCountSum());
            //获取通话总时长
            int durationSum = Integer.valueOf(value.getDurationSum());

            try {
                if (preparedStatement == null) {
                    //初始化preparedStatement
                    preparedStatement = connection.prepareStatement(sql);
                }
                int i = 0;
                //给preparedStatement赋值（根据sql语句）
                preparedStatement.setString(++i, date_contact);
                preparedStatement.setInt(++i, dateId);
                preparedStatement.setInt(++i, contactId);
                preparedStatement.setInt(++i, countSum);
                preparedStatement.setInt(++i, durationSum);
                preparedStatement.setInt(++i, countSum);
                preparedStatement.setInt(++i, durationSum);

                //将sql缓存到客户端
                preparedStatement.addBatch();

                batchSize++;
                if (batchSize >= batchBound) {
                    //批量执行sql
                    preparedStatement.executeBatch();
                    connection.commit();
                    //batchSize归零
                    batchSize = 0;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                if (preparedStatement != null) {
                    preparedStatement.executeBatch();
                    connection.commit();
                }
                JDBCUtil.close(connection, preparedStatement, null);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}

