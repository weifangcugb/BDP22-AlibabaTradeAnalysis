package com.aura.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.*;

/**
 * 将HDFS上的原始数据文件转换成HBase能直接importtsv的文件格式
 */
public class HBaseFile {

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

    public static void main(String[] args) {
        HBaseFile file = new HBaseFile();
        file.exportHBaseFile("E:\\2019光环大数据\\百度网盘\\结业项目\\阿里巴巴口碑商家客流分析系统\\数据\\IJCAI17_original\\dataset\\user_pay.txt",
                "E:\\2019光环大数据\\百度网盘\\结业项目\\阿里巴巴口碑商家客流分析系统\\数据\\IJCAI17_original\\dataset\\user_pay_hbase.txt");
    }
}