package com.aura.action;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aura.basic.BasicActionSupportImpl;
import com.aura.database.JDBCUtils;
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
import java.sql.ResultSet;
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
        //List<String> dateList = prestoToJDBCClient.dayReport(queryDate);
        List<Object> queryResult = new ArrayList<>();
        JSONObject jsonObject = null;
        Connection connection = null;
        Statement stmt = null;
        /*try {

            connection = prestoToJDBCClient.getConnect();
            stmt= connection.createStatement();
            for (String day: dateList
            ) {
                System.out.println("--------day-----"+day);
                jsonObject = retainedAnalysis.CalculateRetention(day, stmt);
                if (jsonObject != null){
                    queryResult.add(jsonObject);
                    System.out.println("-----------save-----"+prestoToJDBCClient.svaeMysql(jsonObject));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            prestoToJDBCClient.close(connection, null, stmt);
        }*/
        //从mysql查询结果
        ResultSet rs = null;
        try {
            connection = JDBCUtils.getConnection();
            stmt = connection.createStatement();
            String sql = "select * from query_result where queryday like '"+queryDate+"%' order by queryday";
            rs = stmt.executeQuery(sql);
            while (rs.next()){
                jsonObject = new JSONObject();
                jsonObject.put("day", rs.getString(1));
                jsonObject.put("day0", rs.getString(2));
                jsonObject.put("day1", rs.getString(3));
                jsonObject.put("day2", rs.getString(4));
                jsonObject.put("day3", rs.getString(5));
                jsonObject.put("day4", rs.getString(6));
                jsonObject.put("day5", rs.getString(7));
                jsonObject.put("day6", rs.getString(8));
                jsonObject.put("day7", rs.getString(9));
                queryResult.add(jsonObject);
                jsonObject = null;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            prestoToJDBCClient.close(connection,rs,stmt);
        }
        JsonHelper.printBasicJsonList(getResponse(), queryResult);
    }

    public RetainedAnalysis getRetainedAnalysis() {
        return retainedAnalysis;
    }

    public PrestoToJDBCClient getPrestoToJDBCClient() {
        return prestoToJDBCClient;
    }
}
