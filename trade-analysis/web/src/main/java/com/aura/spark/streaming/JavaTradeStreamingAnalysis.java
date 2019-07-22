package com.aura.spark.streaming;

import com.aura.database.JavaDBDao;
import com.aura.database.C3P0Utils;
import com.aura.service.ShopInfoService;
import com.aura.util.AuraConfig;
import com.typesafe.config.Config;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

//@Service("streamingAnalysis")
public class JavaTradeStreamingAnalysis {

    private Config config;
    private JavaStreamingContext ssc;

    @Resource
    private ShopInfoService shopInfoService;

    public JavaTradeStreamingAnalysis() {
        config = AuraConfig.getRoot();
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]").setAppName("Trade Streaming Analysis");
        Duration interval = Durations.seconds(AuraConfig.getStreamingConfig().getInt("interval"));
        JavaStreamingContext jsc = new JavaStreamingContext(conf, interval);
        this.ssc = jsc;
    }

    private Map<String, String> getKafkaParams() {
        Map<String, String> params = new HashMap<String, String>();
        Config kafkaConfig = config.getConfig("kafka");
        params.put("metadata.broker.list", kafkaConfig.getString("metadata.broker.list"));
        params.put("auto.offset.reset", kafkaConfig.getString("auto.offset.reset"));
        params.put("group.id", kafkaConfig.getString("group.id"));
        return params;
    }

    //get (shop,city) table from MySQL
    private Map<String,String> getShopCityMap() {
        Map<String,String> shopCityMap = shopInfoService.getShopInfoList();
        if(shopCityMap.isEmpty()) return null;
        return shopCityMap;
    }


    public void runAnalysis() {
        ssc.sparkContext().setLogLevel("WARN");
        String kafkaTopic = AuraConfig.getStreamingConfig().getString("topic");

        //broadcast Map<shopId,cityName>
        Broadcast<Map<String,String>> shopCityBroadCast = ssc.sparkContext().broadcast(getShopCityMap());

        //KafkaDirectStream
        JavaPairInputDStream<String,String> input = KafkaUtils.createDirectStream(
                ssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                getKafkaParams(),
                new HashSet<>(Arrays.asList(kafkaTopic))
        );
        input.foreachRDD(rdd -> {
            //update MySQL, use c3po thread pool
            rdd.foreachPartition(rows -> {
                Connection conn = C3P0Utils.getConnection();
                rows.forEachRemaining(row -> {
                    String shopId = row._2().split(",",-1)[0]; //shopId,payTime
                    try {
                        System.out.println(row._1() + ":" + row._2());
                        //每个商家实时交易次数
                        JavaDBDao.saveMerchantTrade(conn, Integer.valueOf(shopId), 1);

                        //每个城市发生的交易次数
                        String cityName = shopCityBroadCast.getValue().getOrDefault(shopId, null);
                        JavaDBDao.saveCityTrade(conn, cityName.trim(), 1);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                });
            });
        });

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
