package com.aura.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public abstract class HBaseBasic {

    public static Configuration getHbaseConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master");
        conf.set("zookeeper.znode.parent", "/hbase");
        return conf;
    }

    public static Connection getConnection() throws IOException {
        Connection connection;
        //establish the connection to hbase cluster.
        connection = ConnectionFactory.createConnection(getHbaseConf());
        return connection;
    }

    public static Table getTable(String tableName) throws IOException {
        Table table;
        //retrieve a handler to the target table
        table = getConnection().getTable(TableName.valueOf(tableName));
        return table;
    }

    public static void createTable(String tableName, String... cf) throws IOException {
        Admin admin = getConnection().getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for(String columnFamily : cf) {
                HColumnDescriptor columnDescriptor_1 = new HColumnDescriptor(Bytes.toBytes(columnFamily));
                columnDescriptor_1.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor_1);
            }
            admin.createTable(tableDescriptor);
        }
    }




}
