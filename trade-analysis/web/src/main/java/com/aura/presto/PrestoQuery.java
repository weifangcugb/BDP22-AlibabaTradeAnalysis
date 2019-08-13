package com.aura.presto;

import com.aura.database.C3P0Utils;
import com.aura.database.JavaDBDao;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PrestoQuery extends PrestoBasic {

    private static Connection mysql = C3P0Utils.getConnection();


    /**
     * 以城市为单位，统计每个城市总体消费金额
     */
    public void getCityAverageConsumption() throws SQLException {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        String sql = "select a.city_name,sum(a.per_pay) from hive.default.user_pay_orc b join mysql.aura.shop_info a on a.shop_id = b.shop_id group by a.city_name";
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            JavaDBDao.saveCityConsume(mysql,rs.getString(1) , rs.getLong(2));
        }
        rs.close();
        conn.close();


    }

    /**
     * 以天为单位，统计所有商家交易发生次数和被用户浏览次数
     */
    public void getShopTradeView() throws SQLException {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        String sql = "select pay.date,pay.pay_times,view.view_times from " +
                "(select substr(cast(a.pay_time as varchar),1,10) as date,count(*) as pay_times  from hive.default.user_pay_orc a group by substr(cast(a.pay_time as varchar),1,10)) pay " +
                "join " +
                "(select substr(cast(b.view_time as varchar),1,10) as date,count(*) as view_times  from hive.default.user_view_orc b group by substr(cast(b.view_time as varchar),1,10)) view " +
                "on pay.date = view.date order by pay.date desc";
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            JavaDBDao.saveShopTradeView(mysql, rs.getString(1), rs.getInt(2), rs.getInt(3));
        }
        rs.close();
        conn.close();
    }

    /**
     *统计最受欢迎的前10类商品（按照二级分类统计），并输出他们的人均消费
     */
    public void getPopularTop() throws SQLException, UnsupportedEncodingException {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        String sql = "select b.cate_2_name,cast(sum(b.per_pay)*1.0/count(*) as decimal(10,2)) from hive.default.user_pay_orc a join mysql.aura.shop_info b on a.shop_id = b.shop_id " +
                "group by b.cate_2_name order by cast(sum(b.per_pay)*1.0/count(*) as decimal(10,2)) desc limit 10";
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            JavaDBDao.savePopuShopTrade(mysql, new String(rs.getString(1).getBytes(),"utf8"), Double.valueOf(rs.getString(2)));
        }
        rs.close();
        conn.close();
    }

    public static void main(String[] args) throws SQLException, UnsupportedEncodingException {
        PrestoQuery query = new PrestoQuery();
        System.out.println("执行presto查询");
        query.getCityAverageConsumption();
        query.getPopularTop();
        query.getShopTradeView();
    }
}
