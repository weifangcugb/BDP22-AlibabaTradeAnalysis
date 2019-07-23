package com.aura.spark.streaming;

import com.aura.database.JavaDBDao;
import com.aura.database.C3P0Utils;
import com.aura.hbase.HBaseBasic;
import com.aura.service.ShopInfoService;
import com.aura.util.AuraConfig;
import com.typesafe.config.Config;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

//@Service("streamingAnalysis")
public class JavaTradeStreamingAnalysis extends HBaseBasic {

    public static final String TABLE_INFO = "streaming_info";
    private static final String COLUMN_FAMILY = "cf1";
    private static final String QUALIFIER_M = "m";
    private static final String QUALIFIER_C= "c";

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
    private Map<String, String> getShopCityMap() {
        Map<String, String> shopCityMap = shopInfoService.getShopInfoList();
        if (shopCityMap.isEmpty()) return null;
        return shopCityMap;
    }

    public void runAnalysis() {
        ssc.sparkContext().setLogLevel("WARN");
        String kafkaTopic = AuraConfig.getStreamingConfig().getString("topic");

        //broadcast Map<shopId,cityName>
        Broadcast<Map<String, String>> shopCityBroadCast = ssc.sparkContext().broadcast(getShopCityMap());

        //KafkaDirectStream
        JavaPairInputDStream<String, String> input = KafkaUtils.createDirectStream(
                ssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                getKafkaParams(),
                new HashSet<>(Arrays.asList(kafkaTopic))
        );
        input.foreachRDD(rdd -> {
            //1.update MySQL, use c3po thread pool。遇到kafka重复发送消息时，统计结果就会不准确。
            /*rdd.foreachPartition(rows -> { //(userId，[shopId,payTime])
                Connection conn = C3P0Utils.getConnection();
                rows.forEachRemaining(row -> {
                    String shopId = row._2().split(",", -1)[0]; //shopId,payTime
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
            });*/
            //2.update hbase table。定时统计结果表数据，同时设置延迟窗口，处理late date。
            rdd.foreachPartition(rows -> {
                rows.forEachRemaining(row -> {
                    String userId = row._1();
                    String shopId = row._2().split(",", -1)[0];
                    String payTime = row._2().split(",", -1)[1];
                    Table table = null;
                    try {
                        createTable(TABLE_INFO);
                        table = getTable(TABLE_INFO);
                        //每个商家实时交易次数，rowkey以1开始
                        Put merchants = new Put(Bytes.toBytes(shopId));
                        merchants.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(payTime), Bytes.toBytes(userId));
                        table.put(merchants);
                        //每个城市发生的交易次数，rowkey以2开始
                        String cityName = shopCityBroadCast.getValue().getOrDefault(shopId, null);
                        Put cities = new Put(Bytes.toBytes(cityName));
                        merchants.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(payTime), Bytes.toBytes(userId));
                        table.put(cities);
                    } catch (IOException e) {
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
