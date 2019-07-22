package com.aura.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public abstract class Ingest {
    public static final String table_name = "history";
    public static final String column_family_cf1 = "cf1";

    protected Table table;
    private Connection connection;

    public static Configuration getHbaseConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master");
        conf.set("zookeeper.znode.parent", "/hbase");
        return conf;
    }

    public void init() throws IOException {
        //establish the connection to hbase cluster.
        connection = ConnectionFactory.createConnection(getHbaseConf());
        //retrieve a handler to the target table
        table = connection.getTable(TableName.valueOf(table_name));
    }

    public void createTable() throws IOException {
        Admin admin = connection.getAdmin();

        if (!admin.tableExists(TableName.valueOf(table_name))) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(table_name));
            HColumnDescriptor columnDescriptor_1 = new HColumnDescriptor(Bytes.toBytes(column_family_cf1));
            columnDescriptor_1.setMaxVersions(1);
            tableDescriptor.addFamily(columnDescriptor_1);
            admin.createTable(tableDescriptor);
        }
    }


    public void ingest() {
        try {
            init();
            createTable();
            process();
            shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //业务处理逻辑，读HDFS数据存入HBase
    abstract public void process();

    public void shutdown() throws IOException {
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
