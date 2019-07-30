package com.aura.spark.sql;

import com.aura.model.ShopInfo;
import com.aura.model.UserPay;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.Distinct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

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

        //平均日交易额最大的前10个商家，并输出他们各自的交易额 TextFile -> RDD, Dataframe
        //1.Use Java RDD
        Map<String, Long> top10TradePerDay = new HashMap<>();
        JavaPairRDD<Long, String> userPayPair = userPayJavaRDD.mapToPair(line -> {
            return new Tuple2<>(line.getShopId(), line.getPayTime());
        });
        JavaPairRDD<Long, Integer> shopInfoPair = shopInfoJavaRDD.mapToPair(line -> {
            return new Tuple2<>(line.getShopId(), line.getPerPay());
        });


        //2-1.Use SparkSQL
        System.out.println("----------------1----------------");
        shopInfoDF.createOrReplaceTempView("shop_info");
        userPayDF.createOrReplaceTempView("user_pay");
        String sql = "select aa.shopId,CAST(bb.totalPay*1.0/aa.totalTimes AS decimal(10,2)) as average from " +
                "(select shopId,count(payDate) as totalTimes from (select shopId,substr(payTime,1,10) as payDate from user_pay group by shopId,substr(payTime,1,10)) group by shopId) aa" +
                " join (select a.shopId,b.perPay*a.tradeTimes as totalPay from (select shopId,count(*) as tradeTimes from user_pay group by shopId) a join shop_info b on a.shopId = b.shopId) bb" +
                " on aa.shopId = bb.shopId order by CAST(bb.totalPay*1.0/aa.totalTimes AS decimal(10,2)) desc limit 10";

        Dataset<Row> top10TradePerDayRow = spark.sql(sql);
//        top10TradePerDayRow.show();

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

        Dataset<Row> top5PopularMilkTea = consumeDF.filter("cate3Name = '奶茶'")
                .filter("cityName = '北京' or cityName = '上海' or cityName = '广州' or cityName = '深圳'")
                .withColumn("totalScore", col("perPay").divide(5).multiply(0.7).plus(col("totalPay").multiply(0.3).divide(4197347)))
                .orderBy(col("totalScore").desc());
        top5PopularMilkTea.show(10);
        Dataset<Row> top5PopularFastFood = consumeDF.filter("cate3Name = '中式快餐'")
                .filter("cityName = '北京' or cityName = '上海' or cityName = '广州' or cityName = '深圳'")
                .withColumn("totalScore", col("perPay").divide(5).multiply(0.7).plus(col("totalPay").multiply(0.3).divide(4197347)))
                .orderBy(col("totalScore").desc());
        top5PopularFastFood.show(10);
    }

    //Main For Test
    public static void main(String[] args) {
        MerchantsTradeAnalysis merchantsTradeAnalysis = new MerchantsTradeAnalysis();
        merchantsTradeAnalysis.tradeAnalysis();
    }
}
