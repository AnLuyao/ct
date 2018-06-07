package com.util;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author AnLuyao
 * @date 2018-06-05 9:50
 */
public class JDBCInstance {

    private static Connection connection = null;

    private JDBCInstance() {
    }

    public static synchronized Connection getInstance() throws SQLException {
        if (connection == null || connection.isClosed() || !connection.isValid(3)) {
            connection = JDBCUtil.getConnection();
        }
        return connection;
    }
}