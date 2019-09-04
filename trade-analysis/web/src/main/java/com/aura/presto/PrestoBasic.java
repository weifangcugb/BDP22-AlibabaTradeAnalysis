package com.aura.presto;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class PrestoBasic {

    private static String presto_url = "jdbc:presto://master:8081";
    private static String presto_user = "hadoopuser";
    private static String presto_passwd = "hadoopuser";

    protected static Connection getConnection() {
        Connection connection =null;
        try {
            connection = DriverManager.getConnection(presto_url,presto_user,presto_passwd);
            connection.setCatalog("hive");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
