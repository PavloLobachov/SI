package com.epam.samples;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

//public class HbaseClient extends Configured implements Tool {
public class HbaseClient {

    private static final TableName TABLE_NAME = TableName.valueOf("twitter_msg");
    private static final byte[] CF = Bytes.toBytes("data");
    private static final String hbaseHost = "sandbox.hortonworks.com";
    private static final String zookeeperHost = "sandbox.hortonworks.com";
//    private static final String zookeeperHost = "localhost";
//    private static final String hbaseHost = "localhost";


    private Configuration getHBaseConf() {

        Configuration hBaseConfig =  HBaseConfiguration.create();
        hBaseConfig.setInt("timeout", 120000);
//        hBaseConfig.set("hbase.master", hbaseHost+":"+16000);
        hBaseConfig.set("hbase.master", hbaseHost);
        hBaseConfig.set("hbase.zookeeper.quorum", zookeeperHost);
        hBaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure");
        hBaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
        return hBaseConfig;
    }

    public void putTwitt(TableName tableName, String columnFamily, String rowKey, Long id, String date, String text, String user) throws IOException {
        Table table = null;
        try {
            table = getTableConnection(this.getConfiguration(), tableName);
            Put p = new Put(Bytes.toBytes(rowKey));
            p.add(Bytes.toBytes(columnFamily), Bytes.toBytes("id"), Bytes.toBytes(id));
            p.add(Bytes.toBytes(columnFamily), Bytes.toBytes("date"), Bytes.toBytes(date));
            p.add(Bytes.toBytes(columnFamily), Bytes.toBytes("text"), Bytes.toBytes(text));
            p.add(Bytes.toBytes(columnFamily), Bytes.toBytes("user"), Bytes.toBytes(user));
            table.put(p);
        } catch (ServiceException | IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) table.close();
        }
    }

    public Table getTableConnection(Configuration conf, TableName tableName) throws IOException {
        HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
        Table table = null;
        if (hbaseAdmin.tableExists(tableName)){
            table = ConnectionFactory.createConnection(conf).getTable(tableName);
        }
        return table;
    }

    private Configuration getConfiguration() throws ServiceException, IOException {
        Configuration hBaseConfig = getHBaseConf();
        HBaseAdmin.checkHBaseAvailable(hBaseConfig);
        return hBaseConfig;
    }

    //    public int run(String[] argv) throws IOException, ServiceException, TableNotFoundException {
    public int run() throws IOException, ServiceException, TableNotFoundException {
//        setConf(HBaseConfiguration.create(getConf()));

        Configuration hBaseConfig = getHBaseConf();

        HBaseAdmin.checkHBaseAvailable(hBaseConfig);
        HBaseAdmin hbaseAdmin = new HBaseAdmin(hBaseConfig);
//        HBaseAdmin hbaseAdmin = new HBaseAdmin(getConf());
//        hbaseAdmin.checkHBaseAvailable(hBaseConfig);
//        hbaseAdmin.checkHBaseAvailable(getConf());

        if (hbaseAdmin.tableExists(TABLE_NAME)){

            Connection connection = null;
            Table table = null;
            try {
                connection = ConnectionFactory.createConnection(hBaseConfig);
//                connection = ConnectionFactory.createConnection(getConf());
                table = connection.getTable(TABLE_NAME);
                Put p = new Put(Bytes.toBytes("row2"));
                p.add(CF, Bytes.toBytes("id"), Bytes.toBytes("1588"));
                p.add(CF, Bytes.toBytes("date"), Bytes.toBytes("2017-01-04 15:02:54"));
                p.add(CF, Bytes.toBytes("text"), Bytes.toBytes("Test Message"));
                p.add(CF, Bytes.toBytes("user"), Bytes.toBytes("Me"));
                table.put(p);
            } finally {
                if (table != null) table.close();
                if (connection != null) connection.close();
            }
        }else {
            throw new TableNotFoundException("Table twitter_msg not found");
        }
        return 0;
    }

//    public static void main(String[] argv) throws Exception {
//        HbaseClient hc = new HbaseClient();
//        hc.run();
////        hc.run(argv);
////        int ret = ToolRunner.run(hc, argv);
////        System.exit(ret);
//    }

}

class TableNotFoundException extends Exception{
    public TableNotFoundException(String message) {
        super(message);
    }
}

