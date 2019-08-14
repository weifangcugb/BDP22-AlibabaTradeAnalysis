package com.aura.action;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aura.basic.BasicActionSupportImpl;
import com.aura.presto.PrestoToJDBCClient;
import com.aura.presto.RetainedAnalysis;
import com.aura.util.JsonHelper;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Controller("retainedAnalysisAction")
public class RetaindAnalysisAction extends BasicActionSupportImpl {
    @Resource
    RetainedAnalysis retainedAnalysis;
    @Resource
    PrestoToJDBCClient prestoToJDBCClient;

    public void getRetainedList()throws IOException {
        String queryDate = this.getRequest().getParameter("startTime");
        //获取查询月份所有的日期
        System.out.println("--------调用-----"+queryDate);
        List<String> dateList = prestoToJDBCClient.dayReport(queryDate);
        List<Object> queryResult = new ArrayList<>();
        JSONObject jsonObject = new JSONObject();
        Connection connection = null;
        Statement stmt = null;
        try {
            connection = prestoToJDBCClient.getConnect();
            stmt= connection.createStatement();
            for (String day: dateList
            ) {
                System.out.println("--------day-----"+day);
                jsonObject = retainedAnalysis.CalculateRetention(day, stmt);
                if (jsonObject != null){
                    queryResult.add(jsonObject);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            prestoToJDBCClient.close(connection, null, stmt);
        }

        System.out.println("JSONArray.toJSONString(queryResult)::::"+JSONArray.toJSONString(queryResult));
        JsonHelper.printBasicJsonList(getResponse(), queryResult);

    }

    public RetainedAnalysis getRetainedAnalysis() {
        return retainedAnalysis;
    }

    public PrestoToJDBCClient getPrestoToJDBCClient() {
        return prestoToJDBCClient;
    }
}
