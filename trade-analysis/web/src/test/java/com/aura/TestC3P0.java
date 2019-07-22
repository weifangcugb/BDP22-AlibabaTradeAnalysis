package com.aura;


import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.Date;

import javax.sql.DataSource;

import com.aura.database.C3P0Utils;
import com.aura.database.JDBCUtils;
import org.junit.Test;

import com.mchange.v2.c3p0.ComboPooledDataSource;


public class TestC3P0 {

    /**
     * 此方法为抽取工具类的方法
     */
    @Test
    public void testAdd() {
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            // 2.从池子中获取链接
            conn = C3P0Utils.getConnection();
            String sql = "insert into merchant_trade values(?,?,now())" +
                    " ON DUPLICATE KEY UPDATE trade_count = trade_count + 1, update_time = now()";
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,1);
            pstmt.setInt(2, 1);
            int rows = pstmt.executeUpdate();
            if (rows > 0) {
                System.out.println("添加成功");
            } else {
                System.out.println("添加失败");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            JDBCUtils.release(conn, pstmt, null);
        }
    }

}