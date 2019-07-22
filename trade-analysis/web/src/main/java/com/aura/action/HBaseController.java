package com.aura.action;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aura.hbase.HistoryIngest;
import com.aura.hbase.Ingest;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/hbase")
public class HBaseController {

    //给定任意用户ID以及时间区间（比如2015.07.01~2015.07.10）
    @RequestMapping(value = "/search", method = RequestMethod.GET)
    @ResponseBody
    public String searchByUserIdAndDate(@PathVariable("userId") String userId,
        @PathVariable("startTime") String startTime, @PathVariable("endTime") String endTime) throws IOException {
        //establish the connection to the cluster.
        Connection connection = ConnectionFactory.createConnection();
        //retrieve a handler to the target table
        Table table = connection.getTable(TableName.valueOf(Ingest.table_name));

        Scan scan = new Scan();
        //scan: set startkey and endkey
        String startKey = HistoryIngest.userIdCompletion(userId) + HistoryIngest.removeLine(startTime.replace(".",""));
        String endKey = HistoryIngest.userIdCompletion(userId) + HistoryIngest.removeLine(endTime.replace(".",""));
        scan.setStartRow(Bytes.toBytes(startKey)).setStopRow(Bytes.toBytes(endKey));
        scan.setCaching(1000);

        //get result
        ResultScanner rs = table.getScanner(scan);
        List<Object> resList = new ArrayList<>();
        rs.iterator().forEachRemaining(res -> {
            JSONObject jsonObject = new JSONObject();
            for(KeyValue kv : res.raw()) {
                try {
                    String row = new String(res.getRow(),"utf-8");
                    String qualifier = new String(kv.getQualifier(),"utf-8");
                    String value = new String(kv.getValue(),"utf-8");
                    jsonObject.put("row",row);
                    jsonObject.put(qualifier,value);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
            resList.add(jsonObject);
        });
        return JSONArray.toJSONString(resList);
    }

    public static void main(String[] args) throws IOException {
        HBaseController controller = new HBaseController();
        String result = controller.searchByUserIdAndDate("12590929","2016-02-01","2016-04-30");
        System.out.println(result);
    }
}
