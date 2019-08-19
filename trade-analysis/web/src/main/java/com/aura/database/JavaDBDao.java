package com.aura.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class JavaDBDao {

    private static String saveMerchantTradeSql =
            "insert into merchant_trade values(?,?,now()) ON DUPLICATE KEY UPDATE trade_count = trade_count + 1, update_time = now()";

    private static String saveCityTradeSql =
            "insert into city_trade values(?,?,now()) ON DUPLICATE KEY UPDATE trade_count = trade_count + 1, update_time = now()";

    private static String saveTradeAccountSql =
            "insert into trade_acount values(?,?,now())";

    private static String savePopulShopSql =
            "insert into popul_shop values(?,?,?,now())";

    private static String insertShopTrade = "insert into merchant_trade values(?,?,now())";

    private static String deleteShopTrade = "delete from merchant_trade where shop_id = ?";

    private static String insertCityTrade = "insert into city_trade(city_name,trade_count,update_time) values(?,?,now())";

    private static String deleteCityTrade = "delete from city_trade where city_name = ?";

    private static String insertCityConsume = "insert into city_consume values(?,?,now())";

    private static String insertPopuShopTrade = "insert into popu_shop_trade values (?,?)";

    private static String insertShopTradeView = "insert into shop_trade_view values(?,?,?)";

    private static String saveMostViewShopTop50Sql = "insert into most_view_shop values(?,?,?,?)";

    private static String saveRetainedAnalysisSql = "insert into retained_analysis values(?,?,?,?)";

    private static String saveShopViewByDay = "insert into shop_view_analysis1 values(?,?,?,?)";

    private static String deleteShopViewByDay = "delete from shop_view_analysis";


    private static void execute(Connection conn, String sql, Object... params) throws SQLException {
        PreparedStatement pstm = null;
        try {
            pstm = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstm.setObject(i + 1, params[i]);
            }
            pstm.execute();
        } finally {
            DBHelper.close(pstm);
        }
    }

    private static ResultSet executeQuery(Connection conn, String sql, Object... params) throws SQLException {
        PreparedStatement pstm = null;
        ResultSet sets = null;
        try {
            pstm = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstm.setObject(i + 1, params[i]);
            }
            sets = pstm.executeQuery();
        } finally {
            DBHelper.close(pstm);
        }
        return sets;
    }


    public static void saveMerchantTrade(Connection conn, int shopId, int tradeCount) throws SQLException {
        execute(conn, saveMerchantTradeSql, shopId, tradeCount);
    }

    public static void saveCityTrade(Connection conn, String cityName, int tradeCount) throws SQLException {
        execute(conn, saveCityTradeSql, cityName, tradeCount);
    }

    public static void saveTradeAccount(Connection conn, Long shopId, Long tradeCount) throws SQLException {
        execute(conn, saveTradeAccountSql, shopId, tradeCount);
    }

    public static void savePopulShop(Connection conn, Long shopId, String  cate, double grade) throws SQLException {
        execute(conn, savePopulShopSql,shopId, cate, grade);
    }

    public static void saveMostViewShopTop50(Connection conn, Long shopId, String  city, Long pay, Long viewTimes) throws SQLException {
        execute(conn, saveMostViewShopTop50Sql,shopId, city, pay, viewTimes);
    }

    public static void insertOrUpdateM(Connection conn, int shopId, int tradeCount) throws SQLException {
        execute(conn, deleteShopTrade, shopId);
        execute(conn,insertShopTrade,shopId,tradeCount);
    }

    public static void insertOrUpdateC(Connection conn, String cityName, int tradeCount) throws SQLException {
        execute(conn, deleteCityTrade, cityName);
        execute(conn,insertCityTrade,cityName,tradeCount);
    }

    public static void saveCityConsume(Connection conn,String cityName,long consume) throws SQLException {
        execute(conn,insertCityConsume, cityName, consume);
    }

    public static void savePopuShopTrade(Connection conn,String shop,double pay) throws SQLException {
        execute(conn,insertPopuShopTrade, shop, pay);
    }

    public static void saveShopTradeView(Connection conn,String date,Integer pay,Integer view) throws SQLException {
        execute(conn,insertShopTradeView, date, pay,view);
    }

    public static void saveRetainedAnalysis(Connection conn,int shop,String date,String days,double rate) throws SQLException {
        execute(conn,saveRetainedAnalysisSql,shop, date, days, rate);
    }

    public static void saveShopViewByDay(Connection conn,int shop,String date,Long viewTimes,String label) throws SQLException {
        execute(conn,saveShopViewByDay,shop, date, viewTimes, label);
    }

    public static void deleteShopViewByDay(Connection conn) throws SQLException {
        execute(conn,deleteShopViewByDay);
    }

    public static Map<String,String> getShopCityMap() {
        return null;
    }
}
