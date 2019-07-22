package com.aura.database;

import com.aura.database.C3P0Utils;
import com.aura.database.DBHelper;
import com.aura.database.JDBCUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class JavaDBDao {

    private static String saveMerchantTradeSql =
            "insert into merchant_trade values(?,?,now()) ON DUPLICATE KEY UPDATE trade_count = trade_count + 1, update_time = now()";

    private static String saveCityTradeSql =
            "insert into city_trade values(?,?,now()) ON DUPLICATE KEY UPDATE trade_count = trade_count + 1, update_time = now()";

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

    public static void saveMerchantTrade(Connection conn, int shopId, int tradeCount) throws SQLException {
        execute(conn, saveMerchantTradeSql, shopId, tradeCount);
    }

    public static void saveCityTrade(Connection conn, String cityName, int tradeCount) throws SQLException {
        execute(conn, saveCityTradeSql, cityName, tradeCount);
    }

    public static Map<String,String> getShopCityMap() {
        return null;
    }
}
