package com.aura.hbase;

import com.aura.presto.PrestoQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;


import javax.annotation.Resource;
import java.io.*;
import java.sql.SQLException;

import static com.aura.hbase.HistoryIngest.QUALIFIER_NAME_SHOPID;
import static com.aura.hbase.Ingest.column_family_cf1;

/**
 * 将HDFS上的原始数据文件转换成HBase能直接importtsv的文件格式
 */
//@Component
public class HBaseFile {

    @Resource
    PrestoQuery query;

    //优化rowkey后的hbase文件格式
    public void exportHBaseFile(String user_pay, String hbase_file) {
        FileSystem fs = null;
        BufferedReader in = null;
        FileOutputStream outputStream = null;
        Configuration conf = new Configuration();
        Path myPath = new Path(user_pay);
        try {
            fs = myPath.getFileSystem(conf);

            FSDataInputStream hdfsInStream = fs.open(new Path(user_pay));
            outputStream = new FileOutputStream(hbase_file);
            in = new BufferedReader(new InputStreamReader(hdfsInStream));
            String line = null;
            while ((line = in.readLine()) != null) {
                String[] parts = line.split(",", -1);
                String rowkey = HistoryIngest.userIdCompletion(parts[0]) + HistoryIngest.removeLineAndSpace(parts[2].substring(0,13));
                String shopId = parts[1];
                String hbase_line =  rowkey+","+shopId+"\n";
                byte []str = hbase_line.getBytes("utf-8");
                outputStream.write(str);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //未优化的Hbase数据文件
    //rowkey：userid，qualifier：timestamp，value：shopid
    public void exportHBaseFileOutdated(String user_pay, String hbase_file) {
        FileSystem fs = null;
        BufferedReader in = null;
        FileOutputStream outputStream = null;
        Configuration conf = new Configuration();
        Path myPath = new Path(user_pay);
        Connection connection = null;
        try {
            connection  = ConnectionFactory.createConnection(Ingest.getHbaseConf());
            Table table = connection.getTable(TableName.valueOf("history-outdated"));

            fs = myPath.getFileSystem(conf);

            FSDataInputStream hdfsInStream = fs.open(new Path(user_pay));
            outputStream = new FileOutputStream(hbase_file);
            in = new BufferedReader(new InputStreamReader(hdfsInStream));
            String line = null;
            while ((line = in.readLine()) != null) {
                String[] parts = line.split(",", -1);
                String rowkey = parts[0];
                String shopId = parts[1];
                String qualifier = parts[2];
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes(column_family_cf1), Bytes.toBytes(qualifier), Bytes.toBytes(shopId));
                table.put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void updateHBaseUserTable(String user_shop) {
        FileSystem fs = null;
        BufferedReader in = null;
        FileOutputStream outputStream = null;
        Configuration conf = new Configuration();
        Path myPath = new Path(user_shop);
        Connection connection = null;
        try {
            connection  = ConnectionFactory.createConnection(Ingest.getHbaseConf());
            Table table = connection.getTable(TableName.valueOf("user_shop"));

            fs = myPath.getFileSystem(conf);

            FSDataInputStream hdfsInStream = fs.open(new Path(user_shop));
            outputStream = null;
            in = new BufferedReader(new InputStreamReader(hdfsInStream));
            String line = null;
            while ((line = in.readLine()) != null) {
                String[] parts = line.split(":",  -1);
                String rowkey = parts[1];
                String qualifier = parts[2];
                String value = parts[3];
                String recomShopId = query.shopRecommendation(qualifier);
                Put put = new Put(Bytes.toBytes(rowkey.trim()));
                //rowkey:userId, qualifier:shopId, value:recomShopId
                if(recomShopId != null && qualifier != null && rowkey != null) {
                    put.addColumn(Bytes.toBytes(column_family_cf1), Bytes.toBytes(qualifier.trim()), Bytes.toBytes(recomShopId));
                    table.put(put);
                }

            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        HBaseFile file = new HBaseFile();
        file.updateHBaseUserTable("trade-analysis/web/data/user_shop.txt");
    }
}