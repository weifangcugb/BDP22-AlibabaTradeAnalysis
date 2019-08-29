package com.aura;

import com.aura.action.QueryAction;
import com.aura.hbase.HBaseFile;
import com.aura.hbase.HistoryIngest;
import com.aura.service.DimensionService;
import com.aura.spark.streaming.AnnotationQuartz;
import com.aura.spark.streaming.JavaTradeStreamingAnalysis;
import com.aura.util.AuraConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.sql.SQLException;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:spring.xml"})
public class test {

    /*
    @Resource(name="dimensionService")
    private DimensionService dimensionService;


    @Test
    public void testPresto() {
        System.out.println("-------test------");
        String url = AuraConfig.getRoot().getString("presto.url");
        System.out.println(url);
        System.out.println(dimensionService.getStreamStartTime());
    }

    @Resource
    private HistoryIngest ingest;

    @Test
    public void testHbase() {
        System.out.println("hbase");
        ingest.ingest();
    }*/

    @Resource
    QueryAction  action;
    @Test
    public void testQueryAction() throws IOException {
       action.shopRecommendation();
    }

  /*  @Resource
    HBaseFile file;
    @Test
    public void testHbaseShopRecommendation() {
        file.updateHBaseUserTable("hdfs://master:9000/trade-analysis/user_shop.txt");
    }*/

    /*@Resource
    private JavaTradeStreamingAnalysis analysis;

    @Test
    public void testStreaming() {
        analysis.runAnalysis();
    }

    @Resource
    AnnotationQuartz quartz;
    @Test
    public void testAnnotationQuartz() throws IOException, SQLException {
        quartz.HbaseInfoCompact();
    }
*/

}