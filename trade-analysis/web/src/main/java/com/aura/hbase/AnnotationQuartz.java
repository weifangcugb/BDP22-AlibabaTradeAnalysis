package com.aura.hbase;

import com.aura.database.C3P0Utils;
import com.aura.database.JavaDBDao;
import com.aura.spark.streaming.JavaTradeStreamingAnalysis;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 定时任务
 */
@Component
public class AnnotationQuartz extends HBaseBasic {

    private Logger logger = LoggerFactory.getLogger(this.getClass());


    //汇总HBase中存储的SparkStreaming实时信息进MySQL，以便可视化
    @Scheduled(cron = "0 0/10 * * * ?")
    public void HbaseInfoCompact() throws IOException, SQLException {
        logger.info("scheduled task execute");

        Connection conn = C3P0Utils.getConnection();
        Table infoTable = getTable(JavaTradeStreamingAnalysis.TABLE_INFO);
        List<Result> info = getNumRegexRow(infoTable, "0", "99999999", ".*",0 );
        for(Result res : info) {
            List<Cell> cells = res.listCells();
            String row = new String(res.getRow(),"utf-8");
            JavaDBDao.insertOrUpdate(conn,Integer.valueOf(row), cells.size());
            System.out.println("update success");
        }
    }

    /**
     * 根据startRowKey和endRowKey筛选出区间，然后根据regxKey正则匹配和num查出最终的结果
     * @param table 表
     * @param startRowKey 开始的范围
     * @param endRowKey 结束的范围
     * @param regxKey 正则匹配
     * @param num 查询的条数
     * @return List<Result>
     */
    public static List<Result> getNumRegexRow(Table table,String startRowKey,String endRowKey, String regxKey,int num) {
        List<Result> list = null;
        try {
            //创建一个过滤器容器，并设置其关系（AND/OR）
            FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            //设置正则过滤器
            RegexStringComparator rc = new RegexStringComparator(regxKey);
            RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL, rc);
            //过滤获取的条数
            if (num != 0) {
                Filter filterNum = new PageFilter(num);//展示条数，为0时显示全部
                fl.addFilter(filterNum);
            }
            //过滤器的添加
            fl.addFilter(rf);
            Scan scan = new Scan();
            //设置取值范围
            scan.setStartRow(startRowKey.getBytes());//开始的key
            scan.setStopRow(endRowKey.getBytes());//结束的key
            scan.setFilter(fl);//为查询设置过滤器的list
            ResultScanner scanner = table.getScanner(scan) ;
            list = new ArrayList<Result>() ;
            for (Result rs : scanner) {
                list.add(rs) ;
            }
        } catch (Exception e) {
            e.printStackTrace() ;
        }
        finally
        {
            try {
                table.close() ;

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public static void main(String[] args) throws IOException, SQLException {
        AnnotationQuartz annotationQuartz = new AnnotationQuartz();
        annotationQuartz.HbaseInfoCompact();
    }

}
