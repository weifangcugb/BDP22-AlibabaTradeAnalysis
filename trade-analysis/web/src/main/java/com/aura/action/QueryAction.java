package com.aura.action;

import com.alibaba.fastjson.JSONObject;
import com.aura.basic.BasicActionSupportImpl;
import com.aura.hbase.HistoryIngest;
import com.aura.hbase.Ingest;
import com.aura.model.ShopInfo;
import com.aura.model.result.PopulShop;
import com.aura.model.result.TradeAcount;
import com.aura.service.ShopInfoService;
import com.aura.util.JsonHelper;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Controller;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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
        if(userId.isEmpty() || userId.equals("")) {
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
        String startKey = HistoryIngest.userIdCompletion(userId) + HistoryIngest.removeLineAndSpace(startTime.replace(".",""));
        String endKey = HistoryIngest.userIdCompletion(userId) + HistoryIngest.removeLineAndSpace(endTime.replace(".",""));
        scan.setStartRow(Bytes.toBytes(startKey)).setStopRow(Bytes.toBytes(endKey));
        scan.setCaching(1000);

        //get result
        ResultScanner rs = table.getScanner(scan);
        //从hbase中scan符合要求数据，再去MySQL中查维度信息，根据shopId缓存返回结果
        rs.iterator().forEachRemaining(res -> {
            JSONObject jsonObject = new JSONObject();
            for(KeyValue kv : res.raw()) {
                try {
                    String row = new String(res.getRow(),"utf-8");
                    String value = new String(kv.getValue(),"utf-8");
                    ShopInfo info = service.getShopInfoById(Integer.valueOf(value));
                    jsonObject.put("userId", HistoryIngest.removeZero(row.substring(0,8)));
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
     * 实时展示每个商家交易次数
     */
    public void getMerchantTrade() {

    }

    /**
     *  实时展示每个城市发生的交易次数
     */
    public void getCityTrade() {

    }

}
