package com.aura.spark.sql;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aura.database.C3P0Utils;
import com.aura.database.JavaDBDao;
import com.aura.model.ShopInfo;
import com.aura.model.UserPay;
import com.aura.model.UserView;
import com.aura.model.result.ShopTradeView;
import com.mchange.v2.c3p0.impl.C3P0PooledConnection;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.Distinct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Array;
import scala.Tuple2;
import sun.nio.ch.FileKey;

import javax.print.DocFlavor;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class MerchantsTradeAnalysis extends BaseTradeAnalysis {

    private static final Logger logger = LoggerFactory.getLogger(MerchantsTradeAnalysis.class);


    @Override
    protected void tradeAnalysis() {
        JavaRDD<ShopInfo> shopInfoJavaRDD = toShopInfoRDD("trade-analysis/web/data/shop_info.txt");
        shopInfoJavaRDD.cache();
        Dataset<Row> shopInfoDF = toShopInfoDF(shopInfoJavaRDD);
        shopInfoDF.cache();
        JavaRDD<UserPay> userPayJavaRDD = toUserPayRDD("trade-analysis/web/data/user_pay.txt");
        userPayJavaRDD.cache();
        Dataset<Row> userPayDF = toUserPayDF(userPayJavaRDD);
        userPayDF.cache();
        JavaRDD<UserView> userViewJavaRDD = toUserViewRDD("trade-analysis/web/data/user_view.txt");
        Dataset<Row> userViewDF = toUserViewDF(userViewJavaRDD);
        userViewDF.cache();

        ///平均日交易额最大的前10个商家，并输出他们各自的交易额 TextFile -> RDD, Dataframe
        //1.Use Java RDD
        Map<String, Long> top10TradePerDay = new HashMap<>();
        JavaPairRDD<Long, String> userPayPair = userPayJavaRDD.mapToPair(line -> {
            return new Tuple2<>(line.getShopId(), line.getPayTime());
        });
        JavaPairRDD<Long, Integer> shopInfoPair = shopInfoJavaRDD.mapToPair(line -> {
            return new Tuple2<>(line.getShopId(), line.getPerPay());
        });

        Connection connection = C3P0Utils.getConnection();

        //2-1.Use SparkSQL
        System.out.println("----------------1----------------");
        shopInfoDF.createOrReplaceTempView("shop_info");
        userPayDF.createOrReplaceTempView("user_pay");
        userViewDF.createOrReplaceTempView("user_view");
        /*
        String sql = "select aa.shopId,CAST(bb.totalPay*1.0/aa.totalTimes AS decimal(10,2)) as average from " +
                "(select shopId,count(payDate) as totalTimes from (select shopId,substr(payTime,1,10) as payDate from user_pay group by shopId,substr(payTime,1,10)) group by shopId) aa" +
                " join (select a.shopId,b.perPay*a.tradeTimes as totalPay from (select shopId,count(*) as tradeTimes from user_pay group by shopId) a join shop_info b on a.shopId = b.shopId) bb" +
                " on aa.shopId = bb.shopId order by CAST(bb.totalPay*1.0/aa.totalTimes AS decimal(10,2)) desc limit 10";

        Dataset<Row> top10TradePerDayRow = spark.sql(sql);
        top10TradePerDayRow.show(10);

        top10TradePerDayRow.foreachPartition(rows -> {
            Connection connection = C3P0Utils.getConnection();
            rows.forEachRemaining(row -> {
                try {
                    JavaDBDao.saveTradeAccount(connection, row.getInt(0), row.getInt(1));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        });


        //2-2.Use Dataframe
        Dataset<Row> joinDf = userPayDF.join(shopInfoDF, "shopId").select(col("shopId"), col("perPay"),
                substring(col("payTime"), 1, 10).alias("payDate"))
                .groupBy(col("payDate"), col("shopId")).agg(sum(col("perPay")).alias("totalpay"))
                .groupBy("shopId").agg(sum("totalpay").alias("totalPay")).orderBy(desc("totalPay"));
//        joinDf.show();

        System.out.println("----------------2----------------");
        //输出北京、上海、广州和深圳四个城市最受欢迎的5家奶茶商店和中式快餐编号
        Dataset<Row> consumeDF = userPayDF.groupBy(col("shopId")).agg(count("shopId").alias("shopPayCount"))
                .join(shopInfoDF, "shopId").withColumn("totalPay", col("shopPayCount").multiply(col("perPay")))
                .orderBy(col("totalPay").desc());
        consumeDF.show(10);
        long high = consumeDF.select("totalPay").first().getLong(0);
        Dataset<Row> top5PopularMilkTea = consumeDF.filter("cate3Name = '奶茶'")
                .filter("cityName = '北京' or cityName = '上海' or cityName = '广州' or cityName = '深圳'")
                .withColumn("totalScore", col("perPay").divide(5).multiply(0.7).plus(col("totalPay").multiply(0.3).divide(high)))
                .orderBy(col("totalScore").desc());
        top5PopularMilkTea.show(10);
        Dataset<Row> top5PopularFastFood = consumeDF.filter("cate3Name = '中式快餐'")
                .filter("cityName = '北京' or cityName = '上海' or cityName = '广州' or cityName = '深圳'")
                .withColumn("totalScore", col("perPay").divide(5).multiply(0.7).plus(col("totalPay").multiply(0.3).divide(high)))
                .orderBy(col("totalScore").desc());
        top5PopularFastFood.show(10);

        top5PopularMilkTea.foreachPartition(rows -> {
            Connection connection = C3P0Utils.getConnection();
            rows.forEachRemaining(row -> {
                try {
                    JavaDBDao.savePopulShop(connection, row.getInt(0),"奶茶", row.getInt(1));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        });

        top5PopularFastFood.foreachPartition(rows -> {
            Connection connection = C3P0Utils.getConnection();
            rows.forEachRemaining(row -> {
                try {
                    JavaDBDao.savePopulShop(connection, row.getInt(0),"中式快餐", row.getInt(1));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        });
        */

        System.out.println("----------------3----------------");
        //对于平均日交易额最大的前3个商家进行漏斗分析，以浏览行为作为分析目标，输出2016.10.01~2016.10.31共31天的留存率
       /* Dataset<Row> viewDF = spark.sql("select user_id, shop_id , substr(time_stamp,1,10) as view_date from user_view where substr(time_stamp,1,10) <= '2016-10-31' " +
                "and substr(time_stamp,1,10) >= '2016-10-01' and (shop_id = '65')");
        viewDF.createOrReplaceTempView("reten_user_view");
        int start =1, end =31; String prefix = "2016-10-";
        List<String> shopList = Arrays.asList("65","650","1302");
        for(String shopId : shopList) {
            for(int day = start;day <= end; day++) {
                Dataset<Row> c1 = viewDF.filter(col("view_date").equalTo(prefix+day)).select(col("user_id")).distinct();
                System.out.println("c1:" + c1.count());
                for(int date = 0; date + day <= end; date++) {
                    System.out.println("date:"+date);
                    Dataset<Row> c2 = viewDF.filter(col("view_date").equalTo(prefix+date)).select(col("user_id")).distinct();
                    Dataset<Row> intersect = c1.intersect(c2);
                    double rate = 0;
                    if(c1.count() != 0L) { rate = 1.00*c2.count()/c1.count(); }
                    try {
                        JavaDBDao.saveRetainedAnalysis(connection, Integer.valueOf(shopId), String.valueOf(day), String.valueOf(date),rate);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }*/

       /* System.out.println("----------------5----------------");
        //找到被浏览次数最多的50个商家，并输出他们的城市以及人均消费，并选择合适的图表对结果进行可视化
        String viewSql = "select a.shopId,a.cityName,a.perPay,b.viewTimes from shop_info a join (select shop_id,count(*) as viewTimes from user_view group by shop_id order by count(*) desc limit 50) b " +
                "on a.shopId = b.shop_id order by b.viewTimes desc";
        Dataset<Row> mostViewShopTop50 = spark.sql(viewSql);
        mostViewShopTop50.show(50);
        mostViewShopTop50.foreachPartition(rows -> {
            Connection connection = C3P0Utils.getConnection();
            rows.forEachRemaining(row -> {
                try {
                    JavaDBDao.saveMostViewShopTop50(connection, row.getInt(0), row.getString(1), row.getLong(2));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        });*/


    }

    public JSONObject getShopView(String shopId, String startTime, String endTime) {
        JavaRDD<UserView> userViewJavaRDD = toUserViewRDD("trade-analysis/web/data/user_view.txt");
        Dataset<Row> userViewDF = toUserViewDF(userViewJavaRDD).filter(col("shop_id").equalTo(shopId))
                .withColumn("view_time", col("time_stamp").substr(1, 10))
                .filter(col("view_time").geq(startTime)).filter(col("view_time").leq(endTime));
        userViewDF.cache();
        userViewDF.createOrReplaceTempView("user_view");
        Dataset<Row> dayViewDF = spark.sql("select view_time, count(*) from user_view group by view_time, shop_id");
        Dataset<Row> monthViewDF = spark.sql("select substr(view_time,1,7), count(*) from user_view group by substr(view_time,1,7), shop_id");
        dayViewDF.show();
        monthViewDF.show();
        dayViewDF.foreachPartition(rows -> {
            rows.forEachRemaining(row -> {
                ShopTradeView view = new ShopTradeView();
                view.setDate(row.getString(0));
                view.setViewTime(row.getLong(1));
            });
        });
        List<ShopTradeView> monViews = new ArrayList<>();
        monthViewDF.foreachPartition(rows -> {
            rows.forEachRemaining(row -> {
                ShopTradeView view = new ShopTradeView();
                view.setDate(row.getString(0));
                view.setViewTime(row.getLong(1));
                monViews.add(view);
            });
        });
        JSONObject object =new JSONObject();
        object.put("day",null);
        object.put("mon", null);
        return object;
    }
                //Main For Test
        public static void main (String[]args){
            MerchantsTradeAnalysis merchantsTradeAnalysis = new MerchantsTradeAnalysis();
            JSONObject object = merchantsTradeAnalysis.getShopView("1197", "2016-07-25", "2016-09-24");
            System.out.println(object.toJSONString());
        }
    }
