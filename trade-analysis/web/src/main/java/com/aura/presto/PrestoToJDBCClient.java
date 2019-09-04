package com.aura.presto;

import com.alibaba.fastjson.JSONObject;
import com.aura.basic.BasicDao;
import com.aura.database.JDBCUtils;
import com.aura.util.AuraConfig;
import com.typesafe.config.Config;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

//@Service("prestoToJDBCClient")
public abstract class PrestoToJDBCClient {
    private final static String catalog = "hive";
    //获取链接
    public static Connection getConnect(){
        Config config = AuraConfig.getPrestoConfig();
        String driver = config.getString("driver");
        String url = config.getString("url");
        String usename = config.getString("username");
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, usename, null);
            conn.setCatalog(catalog);
        }catch (SQLException e){
            e.printStackTrace();
        }catch (ClassNotFoundException e2){
            e2.printStackTrace();
        }
        return conn;
    }

    //查询平均日交易额最大的三家商家
    public static String getMaxPay(){
        String sql = "select shop_id from shop_info order by per_pay limit 3";
        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;
        String shopIds = "";
        try {
            conn = JDBCUtils.getConnection();
            statement = conn.createStatement();
            rs = statement.executeQuery(sql);
            while (rs.next()){
                shopIds += rs.getString(1)+", ";
            }
        }catch (SQLException ex){
            ex.printStackTrace();
            try{
                //提交失败，执行回滚操作
                conn.rollback();

            }catch (SQLException e) {
                e.printStackTrace();
                System.err.println("回滚执行失败!!!");
            }
            System.err.println("执行失败");

        }finally {
            JDBCUtils.release(conn, null, rs);
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return shopIds.substring(0, shopIds.length()-2);
    }

    //将查询结果保存到mysql
    public int svaeMysql(JSONObject js){
        Connection conn = null;
        int i = 0;
        try{
            String sql = "insert into query_result ( queryday, currentday, firstday, secondday," +
                    "thirthday, fourthday, fivthday, sixthday, seventhday) values('"+js.getString("day")+"'," +
                    "'"+js.getString("day0")+"','"+js.getString("day1")+"','"+js.getString("day2")+"'," +
                    "'"+js.getString("day3")+"','"+js.getString("day4")+"','"+js.getString("day5")+"'," +
                    "'"+js.getString("day6")+"'," +
                    "'"+js.getString("day7")+"')";
            System.out.println("-----savesql ----"+sql);
            conn = JDBCUtils.getConnection();
            i = BasicDao.executeSql(sql, conn);
        }catch (SQLException s){
            s.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (conn != null){
                    conn.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return i;
    }



    //关闭资源
    public static void close(Connection conn, ResultSet rs, Statement statement){
        try {
            if(conn != null){
                conn.close();
            }
            if (rs != null){
                rs.close();
            }
            if(statement != null){
                statement.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    //获取天数
    public static int getDaysByYearMonth(int year, int month) {

        Calendar a = Calendar.getInstance();
        a.set(Calendar.YEAR, year);
        a.set(Calendar.MONTH, month);
        a.set(Calendar.DATE, 1);
        a.roll(Calendar.DATE, -1);
        int maxDate = a.get(Calendar.DATE);
        return maxDate;
    }


    public static List<String> dayReport(String month) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        List<String> list = new ArrayList<>();
        String df = "";
        java.util.Date d = null;
        try {
            cal.setTime(new SimpleDateFormat("yyyy-MM").parse(month));//month 为指定月份任意日期
        }catch (ParseException e){
            e.printStackTrace();
        }
        int year = cal.get(Calendar.YEAR);
        int m = cal.get(Calendar.MONTH);
        cal.set(Calendar.DAY_OF_MONTH, 1);// 从一号开始
        int dayNumOfMonth = getDaysByYearMonth(year, m);
        for (int i = 0; i < dayNumOfMonth; i++, cal.add(Calendar.DATE, 1)) {
            d  = cal.getTime();
            df = simpleDateFormat.format(d);
            list.add(df);
        }
        return list;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        /*Connection conn = DriverManager.getConnection("jdbc:presto://ze:8081", "root", "root");
        conn.setCatalog("hive");
        Statement statement = conn.createStatement();
        ResultSet result = statement.executeQuery("select * from mysql.aura.shop_info limit 10");
        while (result.next()){
            System.out.println(result.getString(1));
        }
        result.close();
        conn.close();*/
        //getMaxPay();
        //dayReport("2019-09");

    }

}
