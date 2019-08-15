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
                byte []str = line.getBytes("utf-8");
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
        file.exportHBaseFile("hdfs://master:9000/trade-analysis/user_pay_55M.txt", "C:\\Users\\weifang\\Desktop\\test.txt");
    }
}