package com.aura.database;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.lang.Exception;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class JDBCUtils {
    private static String driver;
    private static String url;
    private static String username;
    private static String password;

    /**
     * 静态代码块，直接加载，速度更快 这里是创建并获取连接的步骤 并且需要一个名为db.properties：配置文件： 后缀固定，名字自取
     */
    static {

        try {
            // 1.通过当前类获取加载器
            ClassLoader classLoader = JDBCUtils.class.getClassLoader();
            // 2.通过类加载器的方法获得一个输入流
            InputStream is = classLoader.getResourceAsStream("jdbc.properties");
            // 3.创建一个properties对象
            Properties props = new Properties();
            // 4.加载输入流
            props.load(is);
            // 5.获取相关参数的值
            driver = props.getProperty("driver");
            url = props.getProperty("url");
            username = props.getProperty("username");
            password = props.getProperty("password");
        } catch (IOException e) {

            e.printStackTrace();
        }

    }

    /**
     * 这里还需要一个获取连接的方法
     */
    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }

    /**
     * 打开资源入口之后就要关闭 所以这里最后需要关闭资源
     */

    public static void release(Connection conn, PreparedStatement pstmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
