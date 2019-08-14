
package com.aura.presto;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.catalyst.SQLBuilder;
import org.springframework.stereotype.Service;

import java.math.RoundingMode;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * 任务４　留存分析实现
 */
@Service("retainedAnalysis")
public class RetainedAnalysis extends PrestoToJDBCClient{

    private String fristDayRetainedAte = "0.0%";
    private String secondDayRetainedAte = "0.0%";
    private String thirtDayRetainedAte = "0.0%";
    private String forthDayRetainedAte = "0.0%";
    private String fivthDayRetainedAte = "0.0%";
    private String sixthDayRetainedAte = "0.0%";
    private String siventhDayRetainedAte = "0.0%";
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private String shopidstr = null;
    private String[] shopids = null;//平均日交易额最大的前 3 个商家
    public RetainedAnalysis() {
        this.shopidstr = getMaxPay();
        if(this.shopidstr != null && this.shopidstr.contains(",")){
            this.shopids = shopidstr.split(",");
        }else{
            this.shopids = new String[3];
        }

    }
    //分别计算7天的留存率
    public JSONObject CalculateRetention(String viewDate, Statement statement){

        /**
         * 第0天访问的用户在第n天还访问的人数/第０天访问的人数
         *
         */

        String afterday1 = null, afterday2 = null, afterday3 = null, afterday4 = null,
                afterday5 = null,afterday6 = null,afterday7 = null;
        /*Connection conn = null;
        Statement statement = null;*/
        ResultSet rs = null;
        JSONObject jsonObject = new JSONObject();
        afterday1 = getSpecifiedDayAfter(viewDate, 1);
        afterday2 = getSpecifiedDayAfter(viewDate, 2);
        afterday3 = getSpecifiedDayAfter(viewDate, 3);
        afterday4 = getSpecifiedDayAfter(viewDate, 4);
        afterday5 = getSpecifiedDayAfter(viewDate, 5);
        afterday6 = getSpecifiedDayAfter(viewDate, 6);
        afterday7 = getSpecifiedDayAfter(viewDate, 7);
        Map<String, String> result = new HashMap<>();
        StringBuffer sbuffer = new StringBuffer();
        sbuffer.append(" select count(userview.user_id) as day0, ");
        sbuffer.append("count(day1.user_id) as day1, ");
        sbuffer.append("count(day2.user_id) as day2, ");
        sbuffer.append("count(day3.user_id) as day3, ");
        sbuffer.append("count(day4.user_id) as day4, ");
        sbuffer.append("count(day5.user_id) as day5, ");
        sbuffer.append("count(day6.user_id) as day6, ");
        sbuffer.append("count(day7.user_id) as day7 ");

        sbuffer.append(" from aura.user_view_orc userview ");
        //第一天
        sbuffer.append("left join (select user_id from aura.user_view_orc t1 where substr(cast(t1.view_time as ");
        sbuffer.append("varchar),1,10)='"+afterday1+"' and ( t1.shop_id = "+shopids[0]+" or t1.shop_id =  "+shopids[1]+" or t1.shop_id =  "+shopids[2]+"");
        sbuffer.append(")) day1 on userview.user_id=day1.user_id ");

        //第二天
        sbuffer.append("left join (select user_id from aura.user_view_orc t2 where substr(cast(t2.view_time as ");
        sbuffer.append("varchar),1,10)='"+afterday2+"' and ( t2.shop_id = "+shopids[0]+" or t2.shop_id =  "+shopids[1]+" or t2.shop_id =  "+shopids[2]+"");
        sbuffer.append(")) day2 on day1.user_id=day2.user_id ");

        //第三天
        sbuffer.append("left join (select user_id from aura.user_view_orc t3 where substr(cast(t3.view_time as ");
        sbuffer.append("varchar),1,10)='"+afterday3+"' and ( t3.shop_id = "+shopids[0]+" or t3.shop_id =  "+shopids[1]+" or t3.shop_id =  "+shopids[2]+"");
        sbuffer.append(")) day3 on day2.user_id=day3.user_id ");

        //第四天
        sbuffer.append("left join (select user_id from aura.user_view_orc t4 where substr(cast(t4.view_time as ");
        sbuffer.append("varchar),1,10)='"+afterday4+"' and ( t4.shop_id = "+shopids[0]+" or t4.shop_id =  "+shopids[1]+" or t4.shop_id =  "+shopids[2]+"");
        sbuffer.append(")) day4 on day3.user_id=day4.user_id ");

        //第五天
        sbuffer.append("left join (select user_id from aura.user_view_orc t5 where substr(cast(t5.view_time as ");
        sbuffer.append("varchar),1,10)='"+afterday5+"' and ( t5.shop_id = "+shopids[0]+" or t5.shop_id =  "+shopids[1]+" or t5.shop_id =  "+shopids[2]+"");
        sbuffer.append(")) day5 on day4.user_id=day5.user_id ");

        //第六天
        sbuffer.append("left join (select user_id from aura.user_view_orc t6 where substr(cast(t6.view_time as ");
        sbuffer.append("varchar),1,10)='"+afterday6+"' and ( t6.shop_id = "+shopids[0]+" or t6.shop_id =  "+shopids[1]+" or t6.shop_id =  "+shopids[2]+"");
        sbuffer.append(")) day6 on day5.user_id=day6.user_id ");

        //第七天
        sbuffer.append("left join (select user_id from aura.user_view_orc t7 where substr(cast(t7.view_time as ");
        sbuffer.append("varchar),1,10)='"+afterday7+"' and ( t7.shop_id = "+shopids[0]+" or t7.shop_id =  "+shopids[1]+" or t7.shop_id =  "+shopids[2]+"");
        sbuffer.append(")) day7 on day6.user_id=day7.user_id ");


        sbuffer.append(" where substr(cast(userview.view_time as varchar),1,10)=");
        sbuffer.append("'"+viewDate+"'");
        sbuffer.append(" and (");
        sbuffer.append(" userview.shop_id = ");
        sbuffer.append(shopids[0]);
        sbuffer.append(" or userview.shop_id = ");
        sbuffer.append(shopids[1]);
        sbuffer.append(" or userview.shop_id = ");
        sbuffer.append(shopids[2]);
        sbuffer.append(" ) group by substr(cast(userview.view_time as varchar),1,10)");
        try {
            //conn = getConnect();
            //statement = conn.createStatement();
            rs = statement.executeQuery(sbuffer.toString());
            if(rs.next()){
                if(rs.getInt(1) != 0){
                    fristDayRetainedAte = accuracy(rs.getInt(2), rs.getInt(1));
                    secondDayRetainedAte = accuracy(rs.getInt(3), rs.getInt(1));
                    thirtDayRetainedAte = accuracy(rs.getInt(4), rs.getInt(1));
                    forthDayRetainedAte = accuracy(rs.getInt(5), rs.getInt(1));
                    fivthDayRetainedAte = accuracy(rs.getInt(6), rs.getInt(1));
                    sixthDayRetainedAte = accuracy(rs.getInt(7), rs.getInt(1));
                    siventhDayRetainedAte = accuracy(rs.getInt(8), rs.getInt(1));
                }else{
                    fristDayRetainedAte = "0.00%";
                    secondDayRetainedAte = "0.00%";
                    thirtDayRetainedAte = "0.00%";
                    forthDayRetainedAte = "0.00%";
                    fivthDayRetainedAte = "0.00%";
                    sixthDayRetainedAte = "0.00%";
                    siventhDayRetainedAte = "0.00%";
                }
                jsonObject.put("day", viewDate);
                jsonObject.put("day0", "100%");
                jsonObject.put("day1", fristDayRetainedAte);
                jsonObject.put("day2", secondDayRetainedAte);
                jsonObject.put("day3", thirtDayRetainedAte);
                jsonObject.put("day4", forthDayRetainedAte);
                jsonObject.put("day5", fivthDayRetainedAte);
                jsonObject.put("day6", sixthDayRetainedAte);
                jsonObject.put("day7", siventhDayRetainedAte);
            }else{
                jsonObject.put("day", viewDate);
                jsonObject.put("day0", "100%");
                jsonObject.put("day1", "0.00%");
                jsonObject.put("day2", "0.00%");
                jsonObject.put("day3", "0.00%");
                jsonObject.put("day4", "0.00%");
                jsonObject.put("day5", "0.00%");
                jsonObject.put("day6", "0.00%");
                jsonObject.put("day7", "0.00%");
            }
        }catch (SQLException se){
            se.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (rs != null){
                    rs.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }
        return jsonObject;
    }




    /**
     * 获得指定日期的后n天
     * @param specifiedDay
     * @return
     */

    public  String getSpecifiedDayAfter(String specifiedDay, int daynum){
        Calendar c = Calendar.getInstance();
        Date date=null;
        try {
            date = sdf.parse(specifiedDay);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.setTime(date);
        int day=c.get(Calendar.DATE);
        c.set(Calendar.DATE,day + daynum);

        String dayAfter=new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());
        return dayAfter;
    }

    /**
     * 根据日期匹配留存数
     * @param
     */
    private Long filter(List<Map<String, Long>> list, String filter_date){
        Long filter_result =0L;
        for (int i =0; i < list.size(); i++){
            if(list.get(i).get(filter_date) != null){
                filter_result = list.get(i).get(filter_date);
                break;
            }
        }
        return filter_result;
    }


    public  String accuracy(double num, double total) {
        DecimalFormat df = (DecimalFormat) NumberFormat.getInstance();
        //可以设置精确几位小数
        df.setMaximumFractionDigits(2);
        //模式 例如四舍五入
        df.setRoundingMode(RoundingMode.HALF_UP);
        double accuracy_num = num / total * 100;
        return df.format(accuracy_num) + "%";
    }
    public static void main(String[] args) {
        RetainedAnalysis r = new RetainedAnalysis();
        //r.CalculateRetention("2016-06-24");
        try {
            Connection connection = PrestoToJDBCClient.getConnect();
            Statement statement = connection.createStatement();
            r.CalculateRetention("2016-06-22", statement);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

