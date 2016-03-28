package com.epam.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class IndexCooprocessor extends BaseRegionObserver {

    private static final Logger logger = LoggerFactory.getLogger(IndexCooprocessor.class);
    private HTablePool pool = null;

    private final static String  INDEX_TABLE  = "INDEX_TBL";
    private final static String  SOURCE_TABLE = "twitter_msg";

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        logger.info("(start)");
        pool = new HTablePool(env.getConfiguration(), 10);
    }


    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        super.preGetOp(e, get, results);
    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> observerContext, final Put put, final WALEdit edit, final Durability durability) throws IOException {

        byte[] table = observerContext.getEnvironment().getRegion().getRegionInfo().getTableName();
//
//         Not necessary though if you register the coprocessor
//         for the specific table, SOURCE_TBL
        if (!Bytes.equals(table, Bytes.toBytes(SOURCE_TABLE))) {
            return;
        }

        try {
            final List<Cell> idList = put.get(Bytes.toBytes("data"), Bytes.toBytes("id"));
            final List<Cell> dateList = put.get(Bytes.toBytes("data"), Bytes.toBytes("date"));
            Cell id = idList.get(0); //get the column value
            Cell date = dateList.get(0); //get the column value
            System.out.println(id);
            System.out.println(date);
            // get the values
//            HTableInterface idx_table = pool.getTable(Bytes.toBytes(INDEX_TABLE));
//
//            // create row key
//            byte [] rowkey = mkRowKey(null, null); //make the row key
//            Put indexput = new Put(rowkey);
//            indexput.add(
//                    Bytes.toBytes("colfam1"),
//                    Bytes.toBytes("qaul"),
//                    Bytes.toBytes("value.."));
//
//            idx_table.put(indexput);
//            idx_table.close();

        } catch ( IllegalArgumentException ex) {
            logger.error(ex.getMessage());
        }

    }

    private byte[] mkRowKey(Long id, Long timestamp){
        return Bytes.toBytes(String.format("%1$s:%2$s", id, timestamp));
    }


    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        logger.info("(stop)");
        pool.close();
    }


}
