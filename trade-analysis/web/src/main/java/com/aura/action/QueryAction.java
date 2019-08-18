package com.aura.action;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aura.basic.BasicActionSupportImpl;
import com.aura.hbase.HistoryIngest;
import com.aura.hbase.Ingest;
import com.aura.model.*;
import com.aura.model.result.CityConsume;
import com.aura.model.result.MostViewShop;
import com.aura.model.result.PopuShopTrade;
import com.aura.model.result.ShopTradeView;
import com.aura.service.ShopInfoService;
import com.aura.spark.sql.MerchantsTradeAnalysis;
import com.aura.util.JsonHelper;
import javafx.util.converter.DateStringConverter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Controller;

import javax.annotation.Resource;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Controller("queryAction")
public class QueryAction extends BasicActionSupportImpl {

    @Resource
    ShopInfoService service;

    /**
     * HBase查询
     */
    public void getHBaseQueryList() throws IOException {
        List<Object> resList = new ArrayList<>();
        String userId = this.getRequest().getParameter("userId");
        if (userId.isEmpty() || userId.equals("")) {
            JsonHelper.printBasicJsonList(getResponse(), resList);
            return;
        }
        String startTime = this.getRequest().getParameter("startTime");
        String endTime = this.getRequest().getParameter("endTime");

        //establish the connection to the cluster.
        Connection connection = ConnectionFactory.createConnection(Ingest.getHbaseConf());
        //retrieve a handler to the target table
        Table table = connection.getTable(TableName.valueOf(Ingest.table_name));

        Scan scan = new Scan();
        //scan: set startkey and endkey
        String startKey = HistoryIngest.userIdCompletion(userId) + HistoryIngest.removeLineAndSpace(startTime.replace(".", ""));
        String endKey = HistoryIngest.userIdCompletion(userId) + HistoryIngest.removeLineAndSpace(endTime.replace(".", ""));
        scan.setStartRow(Bytes.toBytes(startKey)).setStopRow(Bytes.toBytes(endKey));
        scan.setCaching(1000);

        //get result
        ResultScanner rs = table.getScanner(scan);
        //从hbase中scan符合要求数据，再去MySQL中查维度信息，根据shopId缓存返回结果
        rs.iterator().forEachRemaining(res -> {
            JSONObject jsonObject = new JSONObject();
            for (KeyValue kv : res.raw()) {
                try {
                    String row = new String(res.getRow(), "utf-8");
                    String value = new String(kv.getValue(), "utf-8");
                    ShopInfo info = service.getShopInfoById(Integer.valueOf(value));
                    jsonObject.put("userId", HistoryIngest.removeZero(row.substring(0, 8)));
                    jsonObject.put("payTime", HistoryIngest.formatTime(row.substring(8)));
                    jsonObject.put("info", info);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
            resList.add(jsonObject);
        });
        JsonHelper.printBasicJsonList(getResponse(), resList);
    }

    /**
     * 查询平均日交易额最大的前10个商家
     */
    public void getTradeAccount() {
        List<TradeAcount> list = service.getTradeAcountList();
        JsonHelper.printBasicJsonList(getResponse(), list);
    }

    /**
     * 查询北京、上海、广州和深圳四个城市最受欢迎的5家奶茶商店和中式快餐编号
     */
    public void getPopulShop() {
        String cate = this.getRequest().getParameter("cate");
        List<PopulShop> list = service.getPopulShopList(cate);
        JsonHelper.printBasicJsonList(getResponse(), list);
    }

    /**
     * 实时统计每个商家交易次数
     */
    public void getMerchantTrade() {
        List<MerchantTrade> list = service.getMerchantTradeList();
        JsonHelper.printBasicJsonList(getResponse(), list);
    }

    /**
     * 实时统计每个城市发生的交易次数
     */
    public void getCityTrade() {
        List<CityTrade> list = service.getCityTradeList();
        JsonHelper.printBasicJsonList(getResponse(), list);
    }

    /**
     * 实时统计每个省份发生的交易次数
     */
    public void getProvinceTrade() {
        List<ProvinceTrade> list = service.getProvinceTradeList();
        //将城市成交数据统计为省份数据
        JsonHelper.printBasicJsonList(getResponse(), list);
    }

    /**
     * 更新浏览支付记录,写入文件，实际中可能会同时入库和发送至消息队列
     */
    public void submitTrade() {
        String userId = this.getRequest().getParameter("userId");
        String shopId = this.getRequest().getParameter("shopId");
        String date = new Date().toString();
        try {
            String content = userId+","+shopId+","+ date;
            FileWriter writerPay = new FileWriter("trade-analysis\\web\\data\\user_pay.txt", true);
            FileWriter writerView = new FileWriter("trade-analysis\\web\\data\\user_view.txt", true);
            writerPay.write(content);
            writerView.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonHelper.printBasicJsonList(getResponse(), new ArrayList<>());
    }

    /**
     *统计所有商家交易发生次数和被用户浏览次数
     */
    public void getShopTradeView() {
        List<ShopTradeView> list = service.getShopTradeView();
        JsonHelper.printBasicJsonList(getResponse(), list);
    }

    /**
     *统计每个城市总体消费金额
     */
    public void getCityConsume() {
        List<CityConsume> list = service.getCityConsume();
        JsonHelper.printBasicJsonList(getResponse(), list);
    }

    /**
     *统计最受欢迎的前10类商品（按照二级分类统计），并输出他们的人均消费
     */
    public void getPopuShopTrade() {
        List<PopuShopTrade> list = service.getPopuShopTrade();
        JsonHelper.printBasicJsonList(getResponse(), list);
    }

    /**
     * 给定一个商店（可动态指定），输出该商店每天、每周和每月的被浏览数量
     */
    public void getShopViewByDay() {
        String shopId = this.getRequest().getParameter("shopId");
        String startTime = this.getRequest().getParameter("startTime");
        String stopTime = this.getRequest().getParameter("endTime");

    }

    /**
     * 被浏览次数最多的商家，并输出城市及人均消费
     */
    public void getMostViewShop() {
        List<MostViewShop> list = service.getMostViewShop();
        JsonHelper.printBasicJsonList(getResponse(), list);
    }
}
