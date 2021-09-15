package org.deepglint.util;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * SQL工具类
 *
 * @author ZhangFuQi
 * @date 2021/9/13 13:24
 */
public class DbUtil {
    private static final String URL = "jdbc:mysql://localhost:3306/flink_test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
    private static final String USER = "root";
    private static final String PWD = "example";

    /**
     * 获得数据库连接
     *
     * @return con
     */
    private static Connection getConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(URL, USER, PWD);
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static ArrayList<HashMap<String, String>> executeQuery(String SQL) {
        ArrayList<HashMap<String, String>> list = null;
        HashMap<String, String> hash = null;
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        ResultSetMetaData rsmd = null;
        try {
            conn = getConnection();
            assert conn != null;
            st = conn.createStatement();
            rs = st.executeQuery(SQL);
            rsmd = rs.getMetaData();
            list = new ArrayList<>();
            while (rs.next()) {
                int columnCount = rsmd.getColumnCount();
                hash = new HashMap<>(16);
                for (int i = 1; i <= columnCount; i++) {
                    hash.put(rsmd.getColumnName(i), rs.getString(i));
                }
                list.add(hash);
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(conn, st, rs);
        }
        return null;
    }

    private static void close(Connection conn, Statement st, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
