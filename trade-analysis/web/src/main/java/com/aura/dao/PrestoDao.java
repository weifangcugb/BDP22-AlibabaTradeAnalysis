package com.aura.dao;

import com.aura.util.AuraConfig;
import org.springframework.stereotype.Repository;

import java.sql.*;

@Repository("prestoDao")
public class PrestoDao {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        String driver = AuraConfig.getPrestoConfig().getString("driver");
        String url = AuraConfig.getPrestoConfig().getString("url");
        String username = AuraConfig.getPrestoConfig().getString("username");
        String top_10_cate = "select b.cate_2_name,cast(sum(b.per_pay)*1.0/count(*) as decimal(10,2)) from user_pay_orc a join mysql.aura.shop_info b on a.shop_id = b.shop_id \n" +
                "group by b.cate_2_name order by cast(sum(b.per_pay)*1.0/count(*) as decimal(10,2)) desc limit 10";
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(url,username,null);  ;
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(top_10_cate);
        while (rs.next()) {
            System.out.println(rs.getString(1) + " " + rs.getString(2));
        }
        rs.close();
        connection.close();

    }
}
