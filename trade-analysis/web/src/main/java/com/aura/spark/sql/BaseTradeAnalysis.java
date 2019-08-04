package com.aura.spark.sql;

import com.aura.model.ShopInfo;
import com.aura.model.UserPay;
import com.aura.model.UserView;
import com.aura.util.StringUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 阿里巴巴口碑商家流量分析基础类
 */
abstract  public class BaseTradeAnalysis {

    final protected SparkSession spark;
    final protected JavaSparkContext jsc;

    public BaseTradeAnalysis() {
        SparkConf conf = new SparkConf();
        conf.setIfMissing("spark.app.name", getClass().getSimpleName());
        conf.setMaster("local[2]"); //本地测试使用
        spark = SparkSession.builder().config(conf).getOrCreate();
        spark.sparkContext().setLogLevel("warn");
        jsc = new JavaSparkContext(spark.sparkContext());
    }

    protected abstract void tradeAnalysis();


    protected JavaRDD<ShopInfo> toShopInfoRDD(String path) {
        JavaRDD<String> shopInfo = jsc.textFile(path);
        JavaRDD<ShopInfo> shopInfoJavaRDD = shopInfo.map(line -> {
            String[] parts = line.split(",",-1);
            ShopInfo bean = new ShopInfo();
            bean.setShopId(Long.parseLong(parts[0]));
            bean.setCityName(parts[1]);
            bean.setLocationId(Integer.parseInt(parts[2]));
            bean.setPerPay(Integer.parseInt(parts[3]));
            //针对数据空值处理，如果空设置默认值为0
            bean.setScore(StringUtil.isNotEmpty(parts[4])?Integer.parseInt(parts[4]):0);
            bean.setCommentCnt(StringUtil.isNotEmpty(parts[5])?Integer.parseInt(parts[5]):0);
            bean.setShopLevel(StringUtil.isNotEmpty(parts[6])?Integer.parseInt(parts[6]):0);
            bean.setCate1Name(parts[7]);
            bean.setCate2Name(parts[8]);
            bean.setCate3Name(parts[9]);
            return bean;
        });
        return shopInfoJavaRDD;
    }

    protected Dataset<Row> toShopInfoDF(JavaRDD<ShopInfo> toShopInfoRDD) {
        Dataset<Row> shopInfoRow = spark.createDataFrame(toShopInfoRDD, ShopInfo.class);
        System.out.println("ShopInfo Dataframe Schema");
        shopInfoRow.printSchema();
        return shopInfoRow;
    }

    protected JavaRDD<UserPay> toUserPayRDD(String path) {
        JavaRDD<String> rdd = jsc.textFile(path);
        JavaRDD<UserPay> userPayJavaRDD = rdd.map(line -> {
            String[] parts = line.split(",",-1);
            UserPay bean = new UserPay();
            bean.setUserId(Long.parseLong(parts[0]));
            bean.setShopId(Long.parseLong(parts[1]));
            bean.setPayTime(parts[2]);
            return bean;
        });
        return userPayJavaRDD;
    }

    protected JavaRDD<UserView> toUserViewRDD(String path) {
        JavaRDD<String> rdd = jsc.textFile(path);
        JavaRDD<UserView> userViewJavaRDD = rdd.map(line -> {
            String[] parts = line.split(",",-1);
            UserView bean = new UserView();
            bean.setUserId(Long.parseLong(parts[0]));
            bean.setShopId(Long.parseLong(parts[1]));
            bean.setViewTime(parts[2]);
            return bean;
        });
        return userViewJavaRDD;
    }

    protected Dataset<Row> toUserPayDF(JavaRDD<UserPay> userPayJavaRDD) {
        Dataset<Row> userPayDF = spark.createDataFrame(userPayJavaRDD,UserPay.class);
        System.out.println("UserPay Dataframe Schema");
        userPayDF.printSchema();
        return userPayDF;
    }

    protected Dataset<Row> toUserViewDF(JavaRDD<UserView> userPayJavaRDD) {
        Dataset<Row> userViewDF = spark.createDataFrame(userPayJavaRDD,UserView.class);
        System.out.println("UserView Dataframe Schema");
        userViewDF.printSchema();
        return userViewDF;
    }

}
