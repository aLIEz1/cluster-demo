package org.deepglint.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.deepglint.model.PersonChangeInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * jdbc sink
 *
 * @author ZhangFuQi
 * @date 2021/9/13 10:26
 */
public class MyJdbcSink extends RichSinkFunction<PersonChangeInfo> {
    Connection con = null;
    PreparedStatement insertStmt = null;
    PreparedStatement updateStmt = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        con = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_test?useUnicode=true&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8&useSSL=false", "root", "example");
        insertStmt = con.prepareStatement("insert into change_list(change_from, change_to) value (?,?)");
        updateStmt = con.prepareStatement("update change_list set change_to= ? where change_from = ?");
    }

    @Override
    public void invoke(PersonChangeInfo value, Context context) throws Exception {

        updateStmt.setString(1, value.getTarget());
        updateStmt.setString(2, value.getSource());
        updateStmt.execute();
        if (updateStmt.getUpdateCount() == 0) {
            insertStmt.setString(1, value.getSource());
            insertStmt.setString(2, value.getTarget());
            insertStmt.execute();
        }
    }

    @Override
    public void close() throws Exception {
        insertStmt.close();
        updateStmt.close();
        con.close();
    }
}
