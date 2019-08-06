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

    public static void saveTradeAccount(Connection conn, int shopId, int tradeCount) throws SQLException {
        execute(conn, saveTradeAccountSql, shopId, tradeCount);
    }

    public static void savePopulShop(Connection conn, int shopId, String  cate, double grade) throws SQLException {
        execute(conn, savePopulShopSql,shopId, cate, grade);
    }

    public static void insertOrUpdateM(Connection conn, int shopId, int tradeCount) throws SQLException {
        execute(conn, deleteShopTrade, shopId);
        execute(conn,insertShopTrade,shopId,tradeCount);
    }

    public static void insertOrUpdateC(Connection conn, String cityName, int tradeCount) throws SQLException {
        execute(conn, deleteCityTrade, cityName);
        execute(conn,insertCityTrade,cityName,tradeCount);
    }

    public static Map<String,String> getShopCityMap() {
        return null;
    }
}
